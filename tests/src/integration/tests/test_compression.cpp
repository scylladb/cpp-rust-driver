#include "cassandra.h"
#include "integration.hpp"

class CompressionTests : public Integration {
public:
  CompressionTests() { is_ccm_requested_ = true; }

  void SetUp() {
    Integration::SetUp();
    cluster_ = default_cluster();
  }
};

CASSANDRA_INTEGRATION_TEST_F(CompressionTests, SetCompressionLZ4) {
  cass_cluster_set_compression(cluster_.get(), CASS_COMPRESSION_LZ4);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

CASSANDRA_INTEGRATION_TEST_F(CompressionTests, SetCompressionSnappy) {
  cass_cluster_set_compression(cluster_.get(), CASS_COMPRESSION_SNAPPY);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}

CASSANDRA_INTEGRATION_TEST_F(CompressionTests, SetCompressionNone) {
  cass_cluster_set_compression(cluster_.get(), CASS_COMPRESSION_NONE);
  ASSERT_EQ(CASS_OK, cluster_.connect("", false).connect_error_code());
}
