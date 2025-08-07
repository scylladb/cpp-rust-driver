# Building

The ScyllaDB C/C++ Driver will build on most standard Unix-like platforms.
Packages are available for the platforms listed in [Installation][installation.md].

These packages can be successfully installed on other, compatible systems, but
we do not support such configurations and recommend building from sources
instead.

## Compatibility

* Compilers:
  - rustc 1.82+ (as part of the Rust toolchain, available at [rustup.rs]);
  - any reasonable C/C++ compiler, such as GCC or Clang (for tests & examples).

## Dependencies

The C/C++ driver depends on the following software:

* [CMake] v3.15+
* [libuv] 1.x (only for tests and examples, not required for the driver itself)
* [OpenSSL] versions 1.0.1 through 3.x.x are supported (optional, only if you want to enable TLS support in the driver)

__\*__ Use the `CASS_USE_OPENSSL` CMake option to enable/disable OpenSSL
         support. Disabling this option will disable SSL/TLS protocol support
         within the driver; defaults to `On`.

Note that only `CMake` is mandatory for building the driver. The others:
- `libuv` is only required for building tests and some of the examples, not the driver itself.
- `OpenSSL` is only required if you want to enable TLS support in the driver.

## Linux/MacOS

### Installing dependencies

#### Initial environment setup

##### RHEL/Rocky (dnf)

```bash
dnf install automake cmake gcc-c++ git libtool
```

##### Ubuntu (APT)

```bash
apt update
apt install build-essential cmake git
```

##### Mac OS (Brew)

[Homebrew][Homebrew] (or brew) is a free and open-source software package
management system that simplifies the installation of software on the Mac OS
operating system. Ensure [Homebrew is installed][Homebrew] before proceeding.

```bash
brew update
brew upgrade
brew install autoconf automake cmake libtool
```

#### libuv

**libuv is required only for tests and examples, not required for the driver itself.**
libuv v1.x is recommended. When using a package manager for your operating system,
make sure you install v1.x.

##### Ubuntu

```bash
sudo apt update
sudo apt install libuv-dev
```

##### RHEL/Rocky

```bash
sudo dnf install libuv-devel
```

##### Mac OS (Brew)

```bash
brew install libuv
```

##### Manually build and install

_The following procedures should be performed if packages are not available for
your system._

Browse [https://dist.libuv.org/dist/] and download the newest stable version available.
Follow the instructions in the downloaded package to build and install it.

#### OpenSSL

##### RHEL/Rocky (dnf)

```bash
dnf install openssl-devel
```

##### Ubuntu (APT)

```bash
apt install libssl-dev
```

##### Mac OS (Brew)

```bash
brew install openssl
```

__Note__: For Mac OS X, a link needs to be created in order to make OpenSSL
          available to the building libraries:

```bash
brew link --force openssl
```

##### Manually build and install

Browse [https://openssl-library.org/source/] and download the newest stable version available.
Follow the instructions in the downloaded package to build and install it.

### Building and installing the C/C++ driver

```bash
mkdir build
pushd build
cmake ..
make
make install
popd
```

#### Building examples (optional)

Examples are not built by default and need to be enabled. Update your [CMake]
line to build examples.

```bash
cmake -DCASS_BUILD_EXAMPLES=On ..
```

#### Building tests (optional)

Tests (integration and unit) are not built by default and need to be enabled.

##### All tests

```bash
cmake -DCASS_BUILD_TESTS=On ..
```

__Note__: This will build both the integration and unit tests.

##### Integration tests

```bash
cmake -DCASS_BUILD_INTEGRATION_TESTS=On ..
```

##### Unit tests

```bash
cmake -DCASS_BUILD_UNIT_TESTS=On ..
```

[download server]: https://github.com/scylladb/cpp-rust-driver/releases
[Homebrew]: https://brew.sh
[CMake]: http://www.cmake.org/download
[libuv]: http://libuv.org
[OpenSSL]: https://www.openssl.org
