use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::cluster::metadata::Peer;
use scylla::cluster::{ClusterState, NodeRef};
use scylla::errors::RequestAttemptError;
use scylla::policies::host_filter::HostFilter;
use scylla::policies::load_balancing::{
    DefaultPolicyBuilder, FallbackPlan, LatencyAwarenessBuilder, LoadBalancingPolicy, RoutingInfo,
};
use scylla::statement::Consistency;

/// Whether the LBP allows contacting any hosts in remote datacenters.
/// It strictly corresponds to `allow_hosts_per_remote_dcs` argument of
/// `cass_{cluster,exec_profile}_set_load_balancing_dc_aware()`:
/// - `allow_hosts_per_remote_dcs = 0` <-> `dc_restriction = Local(local_dc)`,
/// - `allow_hosts_per_remote_dcs > 0` <-> `dc_restriction = None`.
#[derive(Clone, Debug)]
pub(crate) enum DcRestriction {
    /// No restriction on datacenters.
    None,
    /// Only the specified local datacenter is allowed.
    Local(String),
}

#[derive(Clone, Debug)]
pub(crate) struct FilteringConfig {
    pub(crate) whitelist_hosts: Vec<IpAddr>,
    pub(crate) blacklist_hosts: Vec<IpAddr>,
    pub(crate) whitelist_dc: Vec<String>,
    pub(crate) blacklist_dc: Vec<String>,
    pub(crate) dc_restriction: DcRestriction,
}

impl FilteringConfig {
    /// Maps each white/blacklist into `Option<Vec<_>>`.
    /// If the list is empty, it is not going to be used for filtering (None).
    fn into_filtering_info(self) -> FilteringInfo {
        FilteringInfo {
            whitelist_hosts: (!self.whitelist_hosts.is_empty()).then_some(self.whitelist_hosts),
            blacklist_hosts: (!self.blacklist_hosts.is_empty()).then_some(self.blacklist_hosts),
            whitelist_dc: (!self.whitelist_dc.is_empty()).then_some(self.whitelist_dc),
            blacklist_dc: (!self.blacklist_dc.is_empty()).then_some(self.blacklist_dc),
            allowed_dcs: match self.dc_restriction {
                DcRestriction::None => AllowedDcs::All,
                DcRestriction::Local(local_dc) => AllowedDcs::Whitelist(vec![local_dc]),
            },
        }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum AllowedDcs {
    /// All datacenters are allowed, including remote ones.
    All,
    /// Only the specified datacenters are allowed (the local DCs of DC-aware policies).
    Whitelist(Vec<String>),
}

#[derive(Debug)]
pub(crate) struct FilteringInfo {
    pub(crate) whitelist_hosts: Option<Vec<IpAddr>>,
    pub(crate) blacklist_hosts: Option<Vec<IpAddr>>,
    pub(crate) whitelist_dc: Option<Vec<String>>,
    pub(crate) blacklist_dc: Option<Vec<String>>,
    /// This is different from `whitelist_dc` in its origin:
    /// - `whitelist_dc` is a user-provided list of datacenters that are allowed,
    /// - `allowed_dcs` is a set built from the load balancing policies, based
    ///   on their allowance of remote datacenters or lack thereof (in case of
    ///   the DC-aware policy with `used_hosts_per_remote_dc=0`).
    pub(crate) allowed_dcs: AllowedDcs,
}

impl FilteringInfo {
    /// Checks if the host is valid according to the filtering rules.
    ///
    /// If host does not belong to any datacenter, its datacenter is treated
    /// as empty string. This way, if for example only `dc1` is whitelisted, the
    /// node with unknown DC will be rejected.
    pub(crate) fn is_host_allowed(&self, ip: &IpAddr, dc: Option<&str>) -> bool {
        // Treat missing dc as empty string.
        let dc = dc.unwrap_or_default();

        if let AllowedDcs::Whitelist(ref allowed_dcs) = self.allowed_dcs {
            // If the host's DC is not in the whitelist of DCs, reject it.
            if !allowed_dcs
                .iter()
                .any(|allowed_dc| allowed_dc.as_str() == dc)
            {
                return false;
            }
        }

        if self
            .whitelist_hosts
            .as_ref()
            .is_some_and(|wl| !wl.contains(ip))
        {
            return false;
        }

        if self
            .blacklist_hosts
            .as_ref()
            .is_some_and(|bl| bl.contains(ip))
        {
            return false;
        }

        if self
            .whitelist_dc
            .as_ref()
            .is_some_and(|wl| !wl.iter().any(|wl_dc| wl_dc.as_str() == dc))
        {
            return false;
        }

        if self
            .blacklist_dc
            .as_ref()
            .is_some_and(|bl| bl.iter().any(|bl_dc| bl_dc.as_str() == dc))
        {
            return false;
        }

        true
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LoadBalancingConfig {
    pub(crate) token_awareness_enabled: bool,
    pub(crate) token_aware_shuffling_replicas_enabled: bool,
    pub(crate) load_balancing_kind: Option<LoadBalancingKind>,
    pub(crate) latency_awareness_enabled: bool,
    pub(crate) latency_awareness_builder: LatencyAwarenessBuilder,
    pub(crate) filtering: FilteringConfig,
}

impl LoadBalancingConfig {
    // This is `async` to prevent running this function from beyond tokio context,
    // as it results in panic due to DefaultPolicyBuilder::build() spawning a tokio task.
    pub(crate) async fn build(self) -> Arc<dyn LoadBalancingPolicy> {
        let load_balancing_kind = self
            .load_balancing_kind
            // Round robin is chosen by default for cluster wide LBP.
            .unwrap_or(LoadBalancingKind::RoundRobin);

        let mut builder = DefaultPolicyBuilder::new().token_aware(self.token_awareness_enabled);
        if self.token_awareness_enabled {
            // Cpp-driver enables shuffling replicas only if token aware routing is enabled.
            builder =
                builder.enable_shuffling_replicas(self.token_aware_shuffling_replicas_enabled);
        }

        // Configure DC-related settings.
        // This includes:
        // - local DC-awareness,
        // - DC failover,
        // - remote DCs allowance for local consistency levels.
        let dc_local_cl_allowance = match load_balancing_kind {
            LoadBalancingKind::DcAware {
                local_dc,
                permit_dc_failover,
                allow_remote_dcs_for_local_cl,
            } => {
                let dc_local_cl_allowance = if allow_remote_dcs_for_local_cl {
                    DcLocalConsistencyAllowance::AllowAll
                } else {
                    DcLocalConsistencyAllowance::DisallowRemotes {
                        local_dc: local_dc.clone(),
                    }
                };
                builder = builder
                    .prefer_datacenter(local_dc)
                    .permit_dc_failover(permit_dc_failover);
                dc_local_cl_allowance
            }
            LoadBalancingKind::RackAware {
                local_dc,
                local_rack,
            } => {
                builder = builder
                    .prefer_datacenter_and_rack(local_dc, local_rack)
                    .permit_dc_failover(true);
                DcLocalConsistencyAllowance::AllowAll
            }
            LoadBalancingKind::RoundRobin => DcLocalConsistencyAllowance::AllowAll,
        };

        if self.latency_awareness_enabled {
            builder = builder.latency_awareness(self.latency_awareness_builder);
        }
        let child_policy = builder.build();

        Arc::new(FilteringLoadBalancingPolicy {
            filtering: self.filtering.into_filtering_info(),
            child_policy,
            dc_local_cl_allowance,
        })
    }
}

impl Default for LoadBalancingConfig {
    fn default() -> Self {
        Self {
            token_awareness_enabled: true,
            token_aware_shuffling_replicas_enabled: true,
            load_balancing_kind: None,
            latency_awareness_enabled: false,
            latency_awareness_builder: Default::default(),
            filtering: FilteringConfig {
                whitelist_hosts: Vec::new(),
                blacklist_hosts: Vec::new(),
                whitelist_dc: Vec::new(),
                blacklist_dc: Vec::new(),
                dc_restriction: DcRestriction::None, // Round-robin policy, which is the default, allows contacting remote DCs.
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum LoadBalancingKind {
    RoundRobin,
    DcAware {
        local_dc: String,
        permit_dc_failover: bool,
        allow_remote_dcs_for_local_cl: bool,
    },
    RackAware {
        local_dc: String,
        local_rack: String,
    },
}

/// Determines whether remote DCs are allowed to be contacted when a local consistency
/// level is used.
#[derive(Debug)]
enum DcLocalConsistencyAllowance {
    /// The policy allows contacting hosts in all datacenters: the local one and remote ones.
    AllowAll,
    /// The policy does not allow contacting hosts in remote datacenters
    /// if a local consistency level is used.
    DisallowRemotes {
        /// The local datacenter, which is the only one that is allowed to be contacted
        /// when a local consistency level is used.
        local_dc: String,
    },
}

impl DcLocalConsistencyAllowance {
    fn is_consistency_local(cl: Consistency) -> bool {
        match cl {
            Consistency::Any
            | Consistency::One
            | Consistency::Two
            | Consistency::Three
            | Consistency::Quorum
            | Consistency::All
            | Consistency::EachQuorum
            | Consistency::Serial => false,
            Consistency::LocalQuorum | Consistency::LocalOne | Consistency::LocalSerial => true,
        }
    }

    fn is_dc_allowed(&self, dc: Option<&str>, cl: Consistency) -> bool {
        match self {
            DcLocalConsistencyAllowance::AllowAll => true,
            DcLocalConsistencyAllowance::DisallowRemotes { local_dc }
                if Self::is_consistency_local(cl) =>
            {
                // If the DC is not the one that is allowed - the local one, return false.
                dc.is_some_and(|dc| local_dc == dc)
            }
            // Consistency is not local, so we allow all datacenters.
            DcLocalConsistencyAllowance::DisallowRemotes { .. } => true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct FilteringLoadBalancingPolicy {
    filtering: FilteringInfo,
    dc_local_cl_allowance: DcLocalConsistencyAllowance,
    child_policy: Arc<dyn LoadBalancingPolicy>,
}

impl LoadBalancingPolicy for FilteringLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(scylla::cluster::NodeRef<'a>, Option<scylla::routing::Shard>)> {
        let picked = self.child_policy.pick(request, cluster);

        tracing::trace!("Child policy pick'd {:?}", picked);

        let our_pick = picked.and_then(|target| {
            let node = target.0;
            let dc = node.datacenter.as_deref();
            (self.filtering.is_host_allowed(&node.address.ip(), dc)
                && self
                    .dc_local_cl_allowance
                    .is_dc_allowed(dc, request.consistency))
            .then_some(target)
        });
        tracing::trace!("Filtering policy pick'd {:?}", our_pick);
        our_pick
    }

    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        Box::new(
            self.child_policy
                .fallback(request, cluster)
                .filter(|(node, _shard)| {
                    let dc = node.datacenter.as_deref();
                    let is_host_allowed = self.filtering.is_host_allowed(&node.address.ip(), dc);
                    let is_dc_allowed = self
                        .dc_local_cl_allowance
                        .is_dc_allowed(dc, request.consistency);
                    tracing::trace!(
                        "Filtering policy got {:?} in fallback and decided to {}.",
                        node,
                        match (is_host_allowed, is_dc_allowed) {
                            (false, false) => "DROP it because neither host nor DC are not allowed",
                            (false, true) => "DROP it because host is not allowed",
                            (true, false) => "DROP it because DC is not allowed",
                            (true, true) => "KEEP it because both host and DC are allowed",
                        }
                    );
                    is_host_allowed && is_dc_allowed
                }),
        )
    }

    fn on_request_success(&self, request: &RoutingInfo, latency: Duration, node: NodeRef<'_>) {
        self.child_policy.on_request_success(request, latency, node);
    }

    fn on_request_failure(
        &self,
        request: &RoutingInfo,
        latency: Duration,
        node: NodeRef<'_>,
        error: &RequestAttemptError,
    ) {
        self.child_policy
            .on_request_failure(request, latency, node, error);
    }

    fn name(&self) -> String {
        format!("FilteringLoadBalancingPolicy({})", self.child_policy.name())
    }
}

/// A host filter used by cpp-rust-driver. It's constructed based on the
/// filtering configuration provided by the user.
pub(crate) struct CassHostFilter {
    pub(crate) filtering: FilteringInfo,
}

impl HostFilter for CassHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        self.filtering
            .is_host_allowed(&peer.address.ip(), peer.datacenter.as_deref())
    }
}

/// Returns the union of all non-empty vectors (sets).
/// If at least one set is empty, it return None.
fn nonempty_union<'a, T>(iter: impl Iterator<Item = &'a Vec<T>>) -> Option<Vec<T>>
where
    T: Clone + PartialEq + 'a,
{
    let mut union = Vec::new();

    for values in iter {
        // If at least one set is empty, we return None.
        if values.is_empty() {
            return None;
        }

        for v in values {
            if !union.contains(v) {
                union.push(v.clone());
            }
        }
    }

    (!union.is_empty()).then_some(union)
}

/// Returns the intersection of all vectors (sets).
/// If the intersection is empty, it returns None.
fn nonempty_intersection<'a, T>(mut iter: impl Iterator<Item = &'a Vec<T>>) -> Option<Vec<T>>
where
    T: Clone + PartialEq + 'a,
{
    // Get the first set (initial intersection).
    let mut intersection = iter.next().cloned();

    // Remove the items from the intersection that are not present in the other sets.
    if let Some(intersection) = intersection.as_mut() {
        for set in iter {
            intersection.retain(|item| set.contains(item));
        }
    }

    // If the intersection is empty, return None.
    intersection.filter(|v| !v.is_empty())
}

impl CassHostFilter {
    /// Computes the filtering rules from the load balancing policies, and
    /// returns a corresponding `HostFilter`.
    ///
    /// In cpp-driver, the following rule is upheld: if a host is rejected
    /// by **all** policies, the connection to that host is not opened at all.
    /// See: https://github.com/scylladb/cpp-driver/blob/fa0f27069a6250/src/request_processor.cpp#L436-L451
    ///
    /// We can achieve this by:
    /// - taking the union of all whitelists (per hosts and per dcs)
    /// - taking the intersection of all blacklists (per hosts and per dcs)
    ///
    /// Now, if a host is not in the union of whitelists, it is rejected.
    /// If a host is in the intersection of blacklists, it is rejected.
    ///
    /// Apart from black- and whitelists, we also compute the allowed datacenters,
    /// which are based on the `used_hosts_per_remote_dc` parameter of the
    /// DC-aware load balancing policy. We only correctly handle the case
    /// where 0 is passed, which means that the policy does not allow contacting
    /// remote datacenters.
    /// If all policies forbid contacting remote datacenters, we prevent
    /// opening connections to hosts in remote datacenters.
    pub(crate) fn new_from_lbp_configs<'a>(
        configs: impl Iterator<Item = &'a LoadBalancingConfig> + Clone,
    ) -> Arc<dyn HostFilter> {
        Arc::new(Self::new_from_lbp_configs_inner(configs))
    }

    /// This is the inner implementation of `new_from_lbp_configs`,
    /// extracted in order to allow testing.
    fn new_from_lbp_configs_inner<'a>(
        configs: impl Iterator<Item = &'a LoadBalancingConfig> + Clone,
    ) -> Self {
        let whitelist_hosts = nonempty_union(
            configs
                .clone()
                .map(|lbp_config| &lbp_config.filtering.whitelist_hosts),
        );

        let blacklist_hosts = nonempty_intersection(
            configs
                .clone()
                .map(|lbp_config| &lbp_config.filtering.blacklist_hosts),
        );

        let whitelist_dc = nonempty_union(
            configs
                .clone()
                .map(|lbp_config| &lbp_config.filtering.whitelist_dc),
        );

        let blacklist_dc = nonempty_intersection(
            configs
                .clone()
                .map(|lbp_config| &lbp_config.filtering.blacklist_dc),
        );

        let allowed_dcs = configs
            .fold(None, |allowed_dcs, lbp_config| {
                match (allowed_dcs, &lbp_config.filtering.dc_restriction) {
                    (None, DcRestriction::None) => Some(AllowedDcs::All),
                    (None, DcRestriction::Local(local_dc)) => {
                        Some(AllowedDcs::Whitelist(vec![local_dc.clone()]))
                    }
                    // If this policy allows only a specified DC, add it to the allowed DCs.
                    (Some(AllowedDcs::Whitelist(mut dcs)), DcRestriction::Local(local_dc)) => {
                        dcs.push(local_dc.clone());
                        Some(AllowedDcs::Whitelist(dcs))
                    }
                    // If some policy allowed all DCs, allow all DCs globally.
                    (Some(AllowedDcs::All), _) => Some(AllowedDcs::All),
                    // If this policy allows all DCs, allow all DCs globally.
                    (_, DcRestriction::None) => Some(AllowedDcs::All),
                }
            })
            // Note: this should never happen, as at least one (cluster-level) policy should be present.
            .unwrap_or(AllowedDcs::All);

        Self {
            filtering: FilteringInfo {
                whitelist_hosts,
                blacklist_hosts,
                whitelist_dc,
                blacklist_dc,
                allowed_dcs,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::load_balancing::LoadBalancingConfig;

    use super::{AllowedDcs, CassHostFilter, DcRestriction, FilteringConfig};

    #[test]
    fn test_union_and_intersection() {
        struct TestCase {
            input: Vec<Vec<i32>>,
            expected_union: Option<Vec<i32>>,
            expected_intersection: Option<Vec<i32>>,
        }

        let test_cases = &[
            TestCase {
                input: vec![],
                expected_union: None,
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![]],
                expected_union: None,
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![], vec![1]],
                expected_union: None,
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![1]],
                expected_union: Some(vec![1]),
                expected_intersection: Some(vec![1]),
            },
            TestCase {
                input: vec![vec![], vec![1], vec![2]],
                expected_union: None,
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![1], vec![2]],
                expected_union: Some(vec![1, 2]),
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![1, 2], vec![2, 3]],
                expected_union: Some(vec![1, 2, 3]),
                expected_intersection: Some(vec![2]),
            },
            TestCase {
                input: vec![vec![1, 2], vec![3, 4]],
                expected_union: Some(vec![1, 2, 3, 4]),
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![1, 2], vec![2, 3], vec![3, 4]],
                expected_union: Some(vec![1, 2, 3, 4]),
                expected_intersection: None,
            },
            TestCase {
                input: vec![vec![1, 2], vec![2, 3], vec![2, 5]],
                expected_union: Some(vec![1, 2, 3, 5]),
                expected_intersection: Some(vec![2]),
            },
        ];

        for test in test_cases {
            let union = super::nonempty_union(test.input.iter());
            let intersection = super::nonempty_intersection(test.input.iter());

            assert_eq!(union, test.expected_union);
            assert_eq!(intersection, test.expected_intersection);
        }
    }

    #[test]
    fn test_allowed_dcs() {
        struct TestCase {
            input: Vec<DcRestriction>,
            merged: AllowedDcs,
        }

        let test_cases = &[
            TestCase {
                input: vec![DcRestriction::None],
                merged: AllowedDcs::All,
            },
            TestCase {
                input: vec![DcRestriction::Local("dc1".to_owned())],
                merged: AllowedDcs::Whitelist(vec!["dc1".to_owned()]),
            },
            TestCase {
                input: vec![
                    DcRestriction::Local("dc1".to_owned()),
                    DcRestriction::Local("dc2".to_owned()),
                ],
                merged: AllowedDcs::Whitelist(vec!["dc1".to_owned(), "dc2".to_owned()]),
            },
            TestCase {
                input: vec![DcRestriction::Local("dc1".to_owned()), DcRestriction::None],
                merged: AllowedDcs::All,
            },
            TestCase {
                input: vec![DcRestriction::None, DcRestriction::Local("dc1".to_owned())],
                merged: AllowedDcs::All,
            },
            TestCase {
                input: vec![
                    DcRestriction::Local("dc1".to_owned()),
                    DcRestriction::None,
                    DcRestriction::Local("dc1".to_owned()),
                ],
                merged: AllowedDcs::All,
            },
            // Non-mandatory case. Should never happen in practice, as at least one (cluster-level) policy should be present.
            TestCase {
                input: vec![],
                merged: AllowedDcs::All,
            },
        ];

        for TestCase { input, merged } in test_cases {
            let filtering_configs = input.iter().map(|dc_restriction| FilteringConfig {
                whitelist_hosts: Vec::new(),
                blacklist_hosts: Vec::new(),
                whitelist_dc: Vec::new(),
                blacklist_dc: Vec::new(),
                dc_restriction: dc_restriction.clone(),
            });
            let load_balancing_configs = filtering_configs
                .map(|filtering| LoadBalancingConfig {
                    token_awareness_enabled: false,
                    token_aware_shuffling_replicas_enabled: false,
                    load_balancing_kind: None,
                    latency_awareness_enabled: false,
                    latency_awareness_builder: Default::default(),
                    filtering,
                })
                .collect::<Vec<_>>();

            let cass_filter =
                CassHostFilter::new_from_lbp_configs_inner(load_balancing_configs.iter());

            assert_eq!(&cass_filter.filtering.allowed_dcs, merged);
        }
    }

    #[test]
    fn test_dc_local_cl_allowance() {
        use super::DcLocalConsistencyAllowance;
        use scylla::statement::Consistency;

        struct TestCase {
            allowance: DcLocalConsistencyAllowance,
            dc: Option<&'static str>,
            cl: Consistency,
            expected: bool,
        }

        let test_cases = vec![
            TestCase {
                allowance: DcLocalConsistencyAllowance::AllowAll,
                dc: Some("dc1"),
                cl: Consistency::LocalQuorum,
                expected: true,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::AllowAll,
                dc: Some("dc1"),
                cl: Consistency::Quorum,
                expected: true,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::DisallowRemotes {
                    local_dc: "dc1".to_owned(),
                },
                dc: Some("dc1"),
                cl: Consistency::LocalQuorum,
                expected: true,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::DisallowRemotes {
                    local_dc: "dc1".to_owned(),
                },
                dc: Some("dc2"),
                cl: Consistency::LocalQuorum,
                expected: false,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::DisallowRemotes {
                    local_dc: "dc1".to_owned(),
                },
                dc: None,
                cl: Consistency::LocalQuorum,
                expected: false,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::DisallowRemotes {
                    local_dc: "dc1".to_owned(),
                },
                dc: Some("dc1"),
                cl: Consistency::Quorum,
                expected: true,
            },
            TestCase {
                allowance: DcLocalConsistencyAllowance::DisallowRemotes {
                    local_dc: "dc1".to_owned(),
                },
                dc: Some("dc2"),
                cl: Consistency::Quorum,
                expected: true,
            },
        ];

        for TestCase {
            allowance,
            dc,
            cl,
            expected,
        } in test_cases
        {
            assert_eq!(allowance.is_dc_allowed(dc, cl), expected);
        }
    }
}
