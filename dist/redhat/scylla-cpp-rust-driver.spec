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
BuildRequires:  cmake
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
%{__rm} -rf scylla-rust-wrapper/.cargo
%{__rm} -rf scylla-rust-wrapper/.rustup
export CARGO_HOME=$(pwd)/scylla-rust-wrapper/.cargo
export RUSTUP_HOME=$(pwd)/scylla-rust-wrapper/.rustup
/usr/bin/curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | RUSTUP_INIT_SKIP_PATH_CHECK=yes /bin/sh -s -- -y

%build
export CARGO_HOME=$(pwd)/scylla-rust-wrapper/.cargo
export RUSTUP_HOME=$(pwd)/scylla-rust-wrapper/.rustup
%cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_VERBOSE_MAKEFILE=ON -DCMAKE_RustFLAGS="-Cforce-frame-pointers=yes --cap-lints=warn"
%cmake_build

%install
%cmake_install

%check
%ctest

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
