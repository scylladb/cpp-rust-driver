#!/bin/bash -e

print_usage() {
    echo "$0 --profile release"
    echo "  --profile   specify profile"
    exit 1
}
PROFILE="release"
while [[ $# -gt 0 ]]; do
    case "$1" in
        "--profile")
            PROFILE="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

TARGET="${PROFILE}"
case "${PROFILE}" in
    "dev")
        TARGET="debug"
        ;;
esac

if [[ ! -d target/"${TARGET}" ]]; then
    echo "Failed to locate build directory: target/${TARGET}"
    exit 1
fi

VERSION_MAJOR=$(sed -n -e 's/^#define CASS_VERSION_MAJOR \(.*\)/\1/p' ../include/cassandra.h)
VERSION_MINOR=$(sed -n -e 's/^#define CASS_VERSION_MINOR \(.*\)/\1/p' ../include/cassandra.h)
VERSION_PATCH=$(sed -n -e 's/^#define CASS_VERSION_PATCH \(.*\)/\1/p' ../include/cassandra.h)
VERSION="${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}"

UNAME="$(uname)"
# On macOS
if [[ "${UNAME}" = "Darwin" ]]; then
    # library name should be "libscylla-cpp-driver", but since Cargo doesn't allow this library name we have to rename it here
    rm -f target/"${TARGET}"/{libscylla-cpp-driver.dylib,libscylla-cpp-driver_static.a}
    cp -v target/"${TARGET}"/libscylla_cpp_driver.dylib target/"${TARGET}"/libscylla-cpp-driver.dylib
    cp -v target/"${TARGET}"/libscylla_cpp_driver.a target/"${TARGET}"/libscylla-cpp-driver_static.a
    rm -fv target/"${TARGET}"/libscylla_cpp_driver.{dylib,a}

    # make .so "versioned" style using symlinks
    rm -f target/"${TARGET}"/libscylla-cpp-driver."${VERSION}".dylib
    cp -v target/"${TARGET}"/libscylla-cpp-driver.dylib target/"${TARGET}"/libscylla-cpp-driver."${VERSION}".dylib
    ln -vsf libscylla-cpp-driver."${VERSION}".dylib target/"${TARGET}"/libscylla-cpp-driver."${VERSION_MAJOR}".dylib
    ln -vsf libscylla-cpp-driver."${VERSION}".dylib target/"${TARGET}"/libscylla-cpp-driver.dylib
# Linux and other
else
    # library name should be "libscylla-cpp-driver", but since Cargo doesn't allow this library name we have to rename it here
    cp --remove-destination -v target/"${TARGET}"/libscylla_cpp_driver.so target/"${TARGET}"/libscylla-cpp-driver.so
    cp --remove-destination -v target/"${TARGET}"/libscylla_cpp_driver.a target/"${TARGET}"/libscylla-cpp-driver_static.a
    rm -fv target/"${TARGET}"/libscylla_cpp_driver.{so,a}

    # make .so "versioned" style using symlinks
    cp --remove-destination -v target/"${TARGET}"/libscylla-cpp-driver.so target/"${TARGET}"/libscylla-cpp-driver.so."${VERSION}"
    ln -vsf libscylla-cpp-driver.so."${VERSION}" target/"${TARGET}"/libscylla-cpp-driver.so."${VERSION_MAJOR}"
    ln -vsf libscylla-cpp-driver.so."${VERSION}" target/"${TARGET}"/libscylla-cpp-driver.so
fi
