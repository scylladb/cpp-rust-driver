#include "integration.hpp"

class ErrorTests : public Integration {
public:
  ErrorTests() { is_ccm_requested_ = false; }
};

CASSANDRA_INTEGRATION_TEST_F(ErrorTests, ServerError) {
  EXPECT_STREQ(cass_error_desc(CASS_ERROR_LAST_ENTRY), "");
  EXPECT_STREQ(cass_error_desc(CASS_OK), "");

  #define XX(source, name, code, desc) \
    EXPECT_STREQ(cass_error_desc(name), desc);
    CASS_ERROR_MAPPING(XX)
  #undef XX
}
