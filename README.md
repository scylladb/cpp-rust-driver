# cpp-rust-driver
Wrapper around ScyllaDB's rust-driver compatible with Datastax cpp-driver (WIP)

# Testing
Integration tests from the original `cpp-driver` are compilable but do not pass yet (also they require libuv, openssl and kerberos to build). To build and run them:

```
mkdir build && cd build
cmake -DCASS_BUILD_INTEGRATION_TESTS=ON  ..
make
./cassandra-integration-tests
```
