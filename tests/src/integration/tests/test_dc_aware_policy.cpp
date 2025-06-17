/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "cassandra.h"
#include "integration.hpp"
#include "objects/error_result.hpp"
#include "objects/execution_profile.hpp"
#include "objects/statement.hpp"

#include "gtest/gtest.h"
#include <algorithm>
#include <iterator>

class DcAwarePolicyTest : public Integration {
public:
  void SetUp() {
    // Create a cluster with 2 DCs with 2 nodes in each
    number_dc1_nodes_ = 2;
    number_dc2_nodes_ = 2;
    is_session_requested_ = false;
    Integration::SetUp();
  }

  void initialize() {
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "text"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "1", "'one'"));
    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "2", "'two'"));
  }

  std::vector<std::string> validate() {
    std::vector<std::string> attempted_hosts, temp;
    Result result;

    result = session_.execute(select_statement("1"));
    temp = result.attempted_hosts();
    std::copy(temp.begin(), temp.end(), std::back_inserter(attempted_hosts));
    EXPECT_EQ(result.first_row().next().as<Varchar>(), Varchar("one"));

    result = session_.execute(select_statement("2"));
    temp = result.attempted_hosts();
    std::copy(temp.begin(), temp.end(), std::back_inserter(attempted_hosts));
    EXPECT_EQ(result.first_row().next().as<Varchar>(), Varchar("two"));

    return attempted_hosts;
  }

  void validate_with_statement(Statement const& statement, bool const expect_no_hosts_available_error, bool const expect_remotes_used) {
    std::vector<std::string> attempted_hosts, temp;
    Result result;

    result = session_.execute(statement, !expect_no_hosts_available_error);
    temp = result.attempted_hosts();
    std::copy(temp.begin(), temp.end(), std::back_inserter(attempted_hosts));

    if (expect_no_hosts_available_error) {
        EXPECT_EQ(result.error_result().error_code(), CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
        return; // No hosts available, so nothing more to check.
    } else {
        // The query should succeed, so we can check the result.
        EXPECT_EQ(result.first_row().next().as<Varchar>(), Varchar("one"));
    }

    if (expect_remotes_used) {
        // Verify that remote DC hosts were used.
        EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "3", attempted_hosts) ||
                    contains(ccm_->get_ip_prefix() + "4", attempted_hosts));

        // Verify that no local DC hosts where used.
        //
        // Commented out, because I'm not sure if this is guaranteed:
        // the driver may have already noticed that the local DC hosts
        // are down and removed them from the host list.
        // EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "1", attempted_hosts) &&
        //             !contains(ccm_->get_ip_prefix() + "2", attempted_hosts));
    } else {
        // Verify that local DC hosts were used.
        EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "1", attempted_hosts) ||
                    contains(ccm_->get_ip_prefix() + "2", attempted_hosts));

        // Verify that no remote DC hosts were used.
        EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "3", attempted_hosts) &&
                    !contains(ccm_->get_ip_prefix() + "4", attempted_hosts));
    }
  }

  Statement select_statement(const std::string& key) {
    Statement statement(
        format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), key.c_str()));
    statement.set_consistency(CASS_CONSISTENCY_ONE);
    statement.set_record_attempted_hosts(true);
    return statement;
  }

  bool contains(const std::string& host, const std::vector<std::string>& attempted_hosts) {
    return std::count(attempted_hosts.begin(), attempted_hosts.end(), host) > 0;
  }
};

/**
 * Verify that the "used hosts per remote DC" setting allows queries to use the
 * remote DC nodes when the local DC nodes are unavailable.
 *
 * This ensures that the DC aware policy correctly uses remote hosts when
 * "used hosts per remote DC" has a value greater than 0.
 *
 * @since 2.8.1
 * @jira_ticket CPP-572
 * @test_category load_balancing_policy:dc_aware
 */
CASSANDRA_INTEGRATION_TEST_F(DcAwarePolicyTest, UsedHostsRemoteDc) {
  CHECK_FAILURE

  // Use up to one of the remote DC nodes if no local nodes are available.
  cluster_ = default_cluster();
  cluster_.with_load_balance_dc_aware("dc1", 1, false);
  connect(cluster_);

  // Create a test table and add test data to it
  initialize();

  { // Run queries using the local DC
    std::vector<std::string> attempted_hosts = validate();

    // Verify that local DC hosts were used
    EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "1", attempted_hosts) ||
                contains(ccm_->get_ip_prefix() + "2", attempted_hosts));

    // Verify that no remote DC hosts were used
    EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "3", attempted_hosts) &&
                !contains(ccm_->get_ip_prefix() + "4", attempted_hosts));
  }

  // Stop the whole local DC
  stop_node(1, true);
  stop_node(2, true);

  { // Run queries using the remote DC
    std::vector<std::string> attempted_hosts = validate();

    // Verify that remote DC hosts were used
    EXPECT_TRUE(contains(ccm_->get_ip_prefix() + "3", attempted_hosts) ||
                contains(ccm_->get_ip_prefix() + "4", attempted_hosts));

    // Verify that no local DC hosts where used
    EXPECT_TRUE(!contains(ccm_->get_ip_prefix() + "1", attempted_hosts) &&
                !contains(ccm_->get_ip_prefix() + "2", attempted_hosts));
  }
}

/**
 * Verify that the "allow remote DCs for local CL" setting allows requests that are using
 * a local consistency level to be routed to remote DC nodes when the local DC nodes
 * are unavailable.
 * Also, verify that requests using a local consistency level are not routed to remote DC nodes
 * when the local DC nodes are unavailable if the "allow remote DCs for local CL" setting is false.
 *
 * @test_category load_balancing_policy:dc_aware
 */
CASSANDRA_INTEGRATION_TEST_F(DcAwarePolicyTest, AllowRemoteDcsForLocalCl) {
  CHECK_FAILURE

  cluster_ = default_cluster();
  char const *const local_dc = "dc1";
  cluster_.with_load_balance_dc_aware(local_dc, 42, false);

  ExecutionProfile profile_allowing_remote_dcs_for_local_cl = ExecutionProfile::build().with_load_balance_dc_aware(local_dc, 42, true);
  char const *const profile_name = "allow_remote_dcs_for_local_cl";
  cluster_.with_execution_profile(profile_name, profile_allowing_remote_dcs_for_local_cl);

  connect(cluster_);

  // Create a test table and add test data to it
  initialize();

  Statement statement_with_nonlocal_consistency = select_statement("1");
  statement_with_nonlocal_consistency.set_consistency(CASS_CONSISTENCY_TWO);

  Statement statement_with_local_consistency = select_statement("1");
  statement_with_local_consistency.set_consistency(CASS_CONSISTENCY_LOCAL_ONE);

  std::cerr << "Testing with `allow_remote_dcs_for_local_cl`=`false`" << std::endl;
  {
    std::cerr << "Running nonlocal statement with local DC available" << std::endl;
    // With local DC available, everything should succeed and only local DC hosts should be used.
    validate_with_statement(statement_with_nonlocal_consistency, false, false);

    std::cerr << "Running local statement with local DC available" << std::endl;
    // With local DC available, everything should succeed and only local DC hosts should be used.
    validate_with_statement(statement_with_local_consistency, false, false);

    // Stop the whole local DC.
    stop_node(1, true);
    stop_node(2, true);

    std::cerr << "Running nonlocal statement with local DC unavailable" << std::endl;
    // With local DC unavailable, remote DC available and nonlocal consistency used, remote hosts should be used and the request should succeed.
    validate_with_statement(statement_with_nonlocal_consistency, false, true);

    std::cerr << "Running local statement with local DC unavailable" << std::endl;
    // With local DC unavailable, remote DC available and local consistency used, remote hosts should be forbidden and the request should fail.
    validate_with_statement(statement_with_local_consistency, true, false);
  }

  statement_with_local_consistency.set_execution_profile(profile_name);
  statement_with_nonlocal_consistency.set_execution_profile(profile_name);
  std::cerr << "Testing with `allow_remote_dcs_for_local_cl`=`true`" << std::endl;
  {
      // The local DC nodes are still dead.

      std::cerr << "Running nonlocal statement with local DC unavailable" << std::endl;
      // With local DC unavailable, remote DC available and nonlocal consistency used, remote hosts should be used and the request should succeed.
      validate_with_statement(statement_with_nonlocal_consistency, false, true);

      std::cerr << "Running local statement with local DC unavailable" << std::endl;
      // With local DC unavailable, remote DC available and local consistency used, remote hosts should be used and the request should succeed.
      validate_with_statement(statement_with_local_consistency, false, true);
  }
}
