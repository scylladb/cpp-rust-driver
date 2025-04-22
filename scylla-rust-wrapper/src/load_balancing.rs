use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use scylla::cluster::{ClusterState, NodeRef};
use scylla::errors::RequestAttemptError;
use scylla::policies::load_balancing::{
    DefaultPolicyBuilder, FallbackPlan, LatencyAwarenessBuilder, LoadBalancingPolicy, RoutingInfo,
};

#[derive(Clone, Debug)]
pub(crate) struct FilteringConfig {
    pub(crate) whitelist_hosts: Vec<IpAddr>,
    pub(crate) blacklist_hosts: Vec<IpAddr>,
    pub(crate) whitelist_dc: Vec<String>,
    pub(crate) blacklist_dc: Vec<String>,
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
        }
    }
}

#[derive(Debug)]
pub(crate) struct FilteringInfo {
    pub(crate) whitelist_hosts: Option<Vec<IpAddr>>,
    pub(crate) blacklist_hosts: Option<Vec<IpAddr>>,
    pub(crate) whitelist_dc: Option<Vec<String>>,
    pub(crate) blacklist_dc: Option<Vec<String>>,
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

        match load_balancing_kind {
            LoadBalancingKind::DcAware { local_dc } => {
                builder = builder.prefer_datacenter(local_dc).permit_dc_failover(true)
            }
            LoadBalancingKind::RackAware {
                local_dc,
                local_rack,
            } => {
                builder = builder
                    .prefer_datacenter_and_rack(local_dc, local_rack)
                    .permit_dc_failover(true)
            }
            LoadBalancingKind::RoundRobin => {}
        }

        if self.latency_awareness_enabled {
            builder = builder.latency_awareness(self.latency_awareness_builder);
        }
        let child_policy = builder.build();

        Arc::new(FilteringLoadBalancingPolicy {
            filtering: self.filtering.into_filtering_info(),
            child_policy,
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
            },
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum LoadBalancingKind {
    RoundRobin,
    DcAware {
        local_dc: String,
    },
    RackAware {
        local_dc: String,
        local_rack: String,
    },
}

#[derive(Debug)]
pub(crate) struct FilteringLoadBalancingPolicy {
    pub(crate) filtering: FilteringInfo,
    pub(crate) child_policy: Arc<dyn LoadBalancingPolicy>,
}

impl LoadBalancingPolicy for FilteringLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(scylla::cluster::NodeRef<'a>, Option<scylla::routing::Shard>)> {
        let picked = self.child_policy.pick(request, cluster);

        picked.and_then(|target| {
            let node = target.0;
            self.filtering
                .is_host_allowed(&node.address.ip(), node.datacenter.as_deref())
                .then_some(target)
        })
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
                    self.filtering
                        .is_host_allowed(&node.address.ip(), node.datacenter.as_deref())
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
