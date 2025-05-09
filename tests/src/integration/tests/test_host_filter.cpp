
#include "integration.hpp"

class HostFilterTest : public Integration {
public:
  HostFilterTest() {
    number_dc1_nodes_ = 2;
    is_session_requested_ = false;
  }
};

CASSANDRA_INTEGRATION_TEST_F(HostFilterTest, DefaultProfileAllNodesBlacklisted) {
  CHECK_FAILURE;

  // The log that rust-driver emits when all nodes are rejected by the HostFilter.
  logger_.add_critera("The host filter rejected all nodes in the cluster, "
                      "no connections that can serve user queries have been established. "
                      "The session cannot serve any queries!");
  // Reject both nodes.
  Cluster cluster = default_cluster().with_blacklist_filtering(Options::host_prefix() + "1," +
                                                               Options::host_prefix() + "2");

  session_ = cluster.connect();

  // Expect an empty plan error.
  Result result =
      session_.execute(Statement("SELECT * FROM system.local WHERE key='local'"), false);

  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, result.error_code());
  ASSERT_TRUE(contains(result.error_message(), "Load balancing policy returned an empty plan."));
  ASSERT_GE(logger_.count(), 1u);
}

CASSANDRA_INTEGRATION_TEST_F(HostFilterTest, MultipleProfilesAllNodesBlacklisted) {
  CHECK_FAILURE;

  // The log that rust-driver emits when all nodes are rejected by the HostFilter.
  logger_.add_critera("The host filter rejected all nodes in the cluster, "
                      "no connections that can serve user queries have been established. "
                      "The session cannot serve any queries!");

  // Add custom execution profile that disables all nodes.
  // Note: profiles_ field is accessed in `default_cluster()` builder method.
  profiles_["blacklist_all"] =
      ExecutionProfile::build()
          .with_blacklist_filtering(Options::host_prefix() + "1," + Options::host_prefix() + "2")
          .with_load_balance_round_robin();
  // Reject both nodes on default exec profile.
  Cluster cluster = default_cluster().with_blacklist_filtering(Options::host_prefix() + "1," +
                                                               Options::host_prefix() + "2");

  session_ = cluster.connect();

  // Expect an empty plan error.
  Result result =
      session_.execute(Statement("SELECT * FROM system.local WHERE key='local'"), false);

  ASSERT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, result.error_code());
  ASSERT_TRUE(contains(result.error_message(), "Load balancing policy returned an empty plan."));
  ASSERT_GE(logger_.count(), 1u);
}
