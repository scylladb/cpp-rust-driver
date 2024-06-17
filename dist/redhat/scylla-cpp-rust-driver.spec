Name:           scylla-cpp-rust-driver
Version:        %{driver_version}
Release:        %{driver_release}%{?dist}
Summary:        ScyllaDB Cpp-Rust Driver
Group:          Development/Tools

License:        LGPLv2.1
URL:            https://github.com/scylladb/cpp-rust-driver
Source0:        %{name}-%{driver_version}-%{driver_release}.tar
BuildRequires:  openssl-devel
BuildRequires:  clang
BuildRequires:  curl
Conflicts:      scylla-cpp-driver

%description
API-compatible rewrite of https://github.com/scylladb/cpp-driver as a wrapper for Rust driver.

%package devel
Summary:        Development libraries for ${name}
Group:          Development/Tools
Requires:       %{name} = %{version}-%{release}
Requires:       pkgconfig
Conflicts:      scylla-cpp-driver-devel

%description devel
Development libraries for %{name}

%prep
%autosetup
%{__rm} -rf scylla-rust-wrapper/target/packaging
%{__rm} -rf scylla-rust-wrapper/.cargo
%{__rm} -rf scylla-rust-wrapper/.rust
/usr/bin/curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | CARGO_HOME=$(pwd)/scylla-rust-wrapper/.cargo RUSTUP_HOME=$(pwd)/scylla-rust-wrapper/.rust RUSTUP_INIT_SKIP_PATH_CHECK=yes /bin/sh -s -- -y
set -euo pipefail

%build
RUSTFLAGS="-Cforce-frame-pointers=yes --cap-lints=warn"
VERSION_MAJOR=$(sed -n -e 's/^#define CASS_VERSION_MAJOR \(.*\)/\1/p' include/cassandra.h)
export RUSTFLAGS="$RUSTFLAGS -Clink-arg=-Wl,-soname=libscylla-cpp-driver.so.$VERSION_MAJOR"
(cd scylla-rust-wrapper && PATH=$(pwd)/.cargo/bin:$PATH CARGO_HOME=$(pwd)/.cargo RUSTUP_HOME=$(pwd)/.rust $(pwd)/.cargo/bin/cargo build %{?_smp_mflags} --profile packaging --verbose)
(cd scylla-rust-wrapper && ./versioning.sh --profile packaging)
sed -e "s#@prefix@#%{_prefix}#g" \
    -e "s#@exec_prefix@#%{_exec_prefix}#g" \
    -e "s#@libdir@#%{_libdir}#g" \
    -e "s#@includedir@#%{_includedir}#g" \
    -e "s#@version@#%{version}#g" \
    dist/common/pkgconfig/scylla-cpp-driver.pc.in > scylla-cpp-driver.pc
sed -e "s#@prefix@#%{_prefix}#g" \
    -e "s#@exec_prefix@#%{_exec_prefix}#g" \
    -e "s#@libdir@#%{_libdir}#g" \
    -e "s#@includedir@#%{_includedir}#g" \
    -e "s#@version@#%{version}#g" \
    dist/common/pkgconfig/scylla-cpp-driver_static.pc.in > scylla-cpp-driver_static.pc

%install
rm -rf %{buildroot}
install -Dpm0644 scylla-rust-wrapper/target/packaging/*.a -t %{buildroot}%{_libdir}
# We need to avoid dereference symlink, so can't use install here
cp -a scylla-rust-wrapper/target/packaging/{*.so.*,*.so} %{buildroot}%{_libdir}
install -Dpm0644 *.pc -t %{buildroot}%{_libdir}/pkgconfig
install -Dpm0644 include/*.h -t %{buildroot}%{_includedir}

%ldconfig_scriptlets

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%doc README.md LICENSE
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root)
%doc README.md LICENSE
%{_includedir}/*.h
%{_libdir}/*.so
%{_libdir}/*.a
%{_libdir}/pkgconfig/*.pc

%changelog
* Thu Mar 28 2024 Takuya ASADA <syuu@scylladb.com>
- initial version of scylla-cpp-rust-driver.spec
