#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --jobs 2 --target rocky-9-x86_64"
    echo "  --jobs  specify number of jobs"
    echo "  --target target distribution in mock cfg name"
    exit 1
}
JOBS="$(nproc)"
TARGET=
while [[ $# -gt 0 ]]; do
    case "$1" in
        "--jobs")
            JOBS="$2"
            shift 2
            ;;
        "--target")
            TARGET="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [[ -f /etc/redhat-release ]]
}

pkg_install() {
    if is_redhat_variant; then
        sudo dnf install -y "$1"
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

TOPDIR="$(pwd)"
if [[ ! -e dist/redhat/build_rpm.sh ]]; then
    echo "run build_rpm.sh in top of scylla dir"
    exit 1
fi

if [[ -z "${TARGET}" ]]; then
    echo "Please specify target"
    exit 1
fi

if [[ ! -f /usr/bin/mock ]]; then
    pkg_install mock
fi
if [[ ! -f /usr/bin/git ]]; then
    pkg_install git
fi

echo "Selected target: ${TARGET}"

./SCYLLA-VERSION-GEN
DRIVER_NAME=scylla-cpp-rust-driver
DRIVER_VERSION="$(sed 's/-/~/' build/SCYLLA-VERSION-FILE)"
DRIVER_RELEASE="$(cat build/SCYLLA-RELEASE-FILE)"

mkdir -p build/redhat/sources
git archive HEAD --prefix "${DRIVER_NAME}-${DRIVER_VERSION}/" --output "build/redhat/sources/${DRIVER_NAME}-${DRIVER_VERSION}-${DRIVER_RELEASE}.tar"
echo "$(cat build/SCYLLA-VERSION-FILE)-$(cat build/SCYLLA-RELEASE-FILE)" > version
tar --xform "s#^#${DRIVER_NAME}-${DRIVER_VERSION}/#" -rf "build/redhat/sources/${DRIVER_NAME}-${DRIVER_VERSION}-${DRIVER_RELEASE}.tar" version

# mock generates files owned by root, fix this up
fix_ownership() {
    CURRENT_UID="$(id -u)"
    CURRENT_GID="$(id -g)"
    sudo chown "${CURRENT_UID}":"${CURRENT_GID}" -R "$@"
}

MOCK_OPTS=(
    -D"driver_version ${DRIVER_VERSION}"
    -D"driver_release ${DRIVER_RELEASE}"
    -D"_smp_mflags -j${JOBS}"
)
sudo mock "${MOCK_OPTS[@]}" --rootdir="${TOPDIR}/build/redhat/mock" --buildsrpm --root="${TARGET}" --resultdir="${TOPDIR}/build/redhat/srpms" --spec="dist/redhat/${DRIVER_NAME}.spec" --sources="build/redhat/sources/${DRIVER_NAME}-${DRIVER_VERSION}-${DRIVER_RELEASE}.tar"
SRPM="$(sed -n -e "s@Wrote: /builddir/build/SRPMS/\(${DRIVER_NAME}-${DRIVER_VERSION//./\.}-${DRIVER_RELEASE}\..*\.src\.rpm\)@\1@p" build/redhat/srpms/build.log | tail -n1)"
sudo mock "${MOCK_OPTS[@]}" --rootdir="${TOPDIR}/build/redhat/mock" --enable-network --rebuild --root="${TARGET}" --resultdir="${TOPDIR}/build/redhat/rpms" "build/redhat/srpms/${SRPM}"
fix_ownership build/redhat/sources
fix_ownership build/redhat/srpms
fix_ownership build/redhat/rpms
