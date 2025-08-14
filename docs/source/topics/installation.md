# Installation

## Packages

Pre-built packages are available for:
- Rocky Linux 9,
- Fedora 41 and 42,
- Ubuntu 22.04 LTS and 24.04 LTS.

### RHEL/Rocky

To install the dependencies:

```bash
sudo dnf -y install libuv openssl
```

Install the runtime library. Replace `<VERSION>` with the version+platform string:

```bash
sudo dnf install -y scylla-cpp-driver-<VERSION>.rpm
```

When developing against the driver you'll also want to install the development
package and the debug symbols:

```bash
sudo dnf install -y scylla-cpp-driver-devel-<VERSION>.rpm scylla-cpp-driver-debuginfo-<VERSION>.rpm
```

### Ubuntu/Debian

Ubuntu's apt-get will resolve and install the dependencies by itself.
Replace `<VERSION>` with the appropriate version+platform string:

```bash
sudo apt-get update
sudo apt-get install -y ./scylla-cpp-driver_<VERSION>.deb
```

When developing against the driver you'll also want to install the development
package and the debug symbols:

```bash
sudo apt-get install -y ./scylla-cpp-driver-dev_<VERSION>.deb ./scylla-cpp-driver-dbg_<VERSION>.deb
```

## Building

If pre-built packages are not available for your platform or architecture,
you will need to build the driver from source. Directions for building and
installing the ScyllaDB C/C++ Driver can be found [here](building.md).
