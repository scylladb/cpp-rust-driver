#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh --jobs 2 --target jammy"
    echo "  --target target distribution codename"
    echo "  --no-clean  don't rebuild pbuilder tgz"
    echo "  --jobs  specify number of jobs"
    exit 1
}

TARGET=
NO_CLEAN=0
JOBS="$(nproc)"
while [ $# -gt 0 ]; do
    case "$1" in
        "--target")
            TARGET=$2
            shift 2
            ;;
        "--no-clean")
            NO_CLEAN=1
            shift 1
            ;;
        "--jobs")
            JOBS=$2
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
is_debian_variant() {
    [[ -f /etc/debian_version ]]
}
is_debian() {
    case "$1" in
        buster|bullseye|bookworm|trixie|sid) return 0;;
        *) return 1;;
    esac
}
is_ubuntu() {
    case "$1" in
        bionic|focal|jammy|mantic|noble) return 0;;
        *) return 1;;
    esac
}


APT_UPDATED=0
declare -A DEB_PKG=(
    ["debian-keyring"]="debian-archive-keyring"
    ["ubu-keyring"]="ubuntu-keyring"
)
pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y "$1"
    elif is_debian_variant; then
        if [[ "${APT_UPDATED}" -eq 0 ]]; then
            sudo apt-get -y update
            APT_UPDATED=1
        fi
        if [[ -n "${DEB_PKG[$1]}" ]]; then
            pkg="${DEB_PKG[$1]}"
        else
            pkg="$1"
        fi
        sudo apt-get install -y "$pkg"
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

if [[ ! -e dist/debian/build_deb.sh ]]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [[ -z "${TARGET}" ]]; then
    echo "Please specify target"
    exit 1
fi

if [[ ! -f /usr/bin/git ]]; then
    pkg_install git
fi
if [[ ! -f /usr/sbin/pbuilder ]]; then
    pkg_install pbuilder
fi
if [[ ! -f /usr/bin/dh_testdir ]]; then
    pkg_install debhelper
fi
if is_debian "${TARGET}" && [[ ! -f /usr/share/keyrings/debian-archive-keyring.gpg ]]; then
    pkg_install debian-keyring
fi
if is_ubuntu "${TARGET}" && [[ ! -f /usr/share/keyrings/ubuntu-archive-keyring.gpg ]]; then
   pkg_install ubu-keyring
fi

./SCYLLA-VERSION-GEN
DRIVER_NAME=scylla-cpp-rust-driver
DRIVER_VERSION="$(sed 's/-/~/' build/SCYLLA-VERSION-FILE)"
DRIVER_RELEASE="$(cat build/SCYLLA-RELEASE-FILE)"
ARCH="$(dpkg --print-architecture)"

mkdir -p build/debian/debs
git archive HEAD --prefix "${DRIVER_NAME}-${DRIVER_VERSION}/" --output "build/debian/${DRIVER_NAME}_${DRIVER_VERSION}-${DRIVER_RELEASE}.orig.tar"
echo "$(cat build/SCYLLA-VERSION-FILE)-$(cat build/SCYLLA-RELEASE-FILE)" > version
tar --xform "s#^#${DRIVER_NAME}-${DRIVER_VERSION}/#" -rf "build/debian/${DRIVER_NAME}_${DRIVER_VERSION}-${DRIVER_RELEASE}.orig.tar" version
gzip -f --fast "build/debian/${DRIVER_NAME}_${DRIVER_VERSION}-${DRIVER_RELEASE}.orig.tar"

if is_debian "${TARGET}"; then
    DRIVER_REVISION="1~${TARGET}"
elif is_ubuntu "${TARGET}"; then
    DRIVER_REVISION="0ubuntu1~${TARGET}"
else
   echo "Unknown distribution: ${TARGET}"
fi
tar xpvf "build/debian/${DRIVER_NAME}_${DRIVER_VERSION}-${DRIVER_RELEASE}.orig.tar.gz" -C build/debian

./dist/debian/debian_files_gen.py --version "${DRIVER_VERSION}" --release "${DRIVER_RELEASE}" --revision "${DRIVER_REVISION}" --codename "${TARGET}" --output-dir "build/debian/${DRIVER_NAME}"-"${DRIVER_VERSION}"/debian

# pbuilder generates files owned by root, fix this up
fix_ownership() {
    CURRENT_UID="$(id -u)"
    CURRENT_GID="$(id -g)"
    sudo chown "${CURRENT_UID}":"${CURRENT_GID}" -R "$@"
}

# use from pbuilderrc
if [[ "${NO_CLEAN}" -eq 0 ]]; then
    sudo rm -fv "/var/cache/pbuilder/${DRIVER_NAME}-${TARGET}-${ARCH}-base.tgz"
    sudo mkdir -p "/var/cache/pbuilder/${DRIVER_NAME}-${TARGET}-${ARCH}/aptcache"
    sudo DRIVER_NAME="${DRIVER_NAME}" DIST="${TARGET}" ARCH="${ARCH}" /usr/sbin/pbuilder clean --configfile ./dist/debian/pbuilderrc
    sudo DRIVER_NAME="${DRIVER_NAME}" DIST="${TARGET}" ARCH="${ARCH}" /usr/sbin/pbuilder create --configfile ./dist/debian/pbuilderrc
fi
sudo DRIVER_NAME="${DRIVER_NAME}" DIST="${TARGET}" ARCH="${ARCH}" /usr/sbin/pbuilder update --configfile ./dist/debian/pbuilderrc
(cd "build/debian/${DRIVER_NAME}"-"${DRIVER_VERSION}"; dpkg-source -b .)
sudo DRIVER_NAME="${DRIVER_NAME}" DIST="${TARGET}" ARCH="${ARCH}" /usr/sbin/pbuilder build --configfile ./dist/debian/pbuilderrc --buildresult build/debian/debs "build/debian/${DRIVER_NAME}_${DRIVER_VERSION}-${DRIVER_RELEASE}-${DRIVER_REVISION}.dsc"
fix_ownership build/debian/debs
