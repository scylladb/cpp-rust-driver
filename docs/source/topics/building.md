# Building

The ScyllaDB C/C++ Driver will build on most standard Unix-like and Microsoft
Windows platforms. Packages are available for the following platforms:

* [CentOS 7 64-bit][cpp-driver-centos7]
* [Ubuntu 18.04 LTS 64-bit][cpp-driver-ubuntu18-04]

These packages can be successfully installed on other, compatible systems, but
we do not support such configurations and recommend building from sources
instead. Please note that although Microsoft Windows and OS X builds are possible,
ScyllaDB does not support these platforms.

## Compatibility

* Compilers: GCC 4.1.2+ Clang 3.4+, and MSVC 2012+

## Dependencies

The C/C++ driver depends on the following software:

* [CMake] v2.6.4+
* [libuv] 1.x (only for tests and examples, not required for the driver itself)
* [OpenSSL] v1.0.x or v1.1.x \*

__\*__ Use the `CASS_USE_OPENSSL` CMake option to enable/disable OpenSSL
         support. Disabling this option will disable SSL/TLS protocol support
         within the driver; defaults to `On`.

Note that only `CMake` is mandatory for building the driver. The others:
- `libuv` is only required for building tests and some of the examples, not the driver itself.
- `OpenSSL` is only required if you want to enable TLS support in the driver.

## Linux/Mac OS

The driver is known to build on CentOS/RHEL 6/7/8, Mac OS X 10.10/10.11 (Yosemite
and El Capitan), Mac OS 10.12/10.13 (Sierra and High Sierra), and Ubuntu
14.04/16.04/18.04 LTS.

__NOTE__: The driver will also build on most standard Unix-like systems using
          GCC 4.1.2+ or Clang 3.4+.

### Installing dependencies

#### Initial environment setup

##### CentOS/RHEL (Yum)

```bash
yum install automake cmake gcc-c++ git libtool
```

##### Ubuntu (APT)

```bash
apt-get update
apt-get install build-essential cmake git
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
sudo apt-get update
sudo apt-get install libuv-dev
```

##### CentOS/RHEL

```bash
sudo dnf install libuv-devel
```

If your package manager is not able to locate `libuv`, you might still be able to
install it from EPEL:

```bash
sudo yum install -y epel-release
sudo yum install -y libuv-devel
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

##### CentOS (Yum)

```bash
yum install openssl-devel
```

##### Ubuntu (APT)

```bash
apt-get install libssl-dev
```

##### Mac OS (Brew)

```bash
brew install openssl
```

__Note__: For Mac OS X 10.11 (El Capitan) and Mac OS 10.12/10.13 (Sierra and
          High Sierra) a link needs to be created in order to make OpenSSL
          available to the building libraries:

```bash
brew link --force openssl
```

##### Manually build and install

```bash
pushd /tmp
wget --no-check-certificate https://www.openssl.org/source/openssl-1.0.2u.tar.gz
tar xzf openssl-1.0.2u.tar.gz
pushd openssl-1.0.2u
CFLAGS=-fpic ./config shared
make install
popd
popd
```

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

__Note__: This will build both the integration and unit tests

##### Integration tests

```bash
cmake -DCASS_BUILD_INTEGRATION_TESTS=On ..
```

##### Unit tests

```bash
cmake -DCASS_BUILD_UNIT_TESTS=On ..
```

[download server]: https://github.com/scylladb/cpp-rust-driver/releases
[cpp-driver-centos7]: https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver-2.15.2-1.el7.x86_64.rpm
[cpp-driver-ubuntu18-04]: https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver_2.15.2-1_amd64.deb
[Homebrew]: https://brew.sh
[CMake]: http://www.cmake.org/download
[libuv]: http://libuv.org
[OpenSSL]: https://www.openssl.org
