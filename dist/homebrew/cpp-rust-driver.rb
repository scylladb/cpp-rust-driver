require "formula"

class CppRustDriver < Formula
  homepage "https://github.com/scylladb/cpp-rust-driver"
  head "https://github.com/scylladb/cpp-rust-driver.git", branch: "master"

  depends_on "openssl"
  depends_on "rust"

  def install
    cass_header = File.open('include/cassandra.h').read()
    version_major = cass_header.match(/^#define CASS_VERSION_MAJOR (.+)$/)[1]
    version_minor = cass_header.match(/^#define CASS_VERSION_MINOR (.+)$/)[1]
    version_patch = cass_header.match(/^#define CASS_VERSION_PATCH (.+)$/)[1]
    version = "#{version_major}.#{version_minor}.#{version_patch}"
    puts version

    ENV["RUSTFLAGS"] = "-Clink-arg=-Wl,-install_name,#{lib}/libscylla-cpp-driver.#{version_major}.dylib -Clink-arg=-Wl,-current_version,#{version} -Clink-arg=-Wl,-compatibility_version,#{version_major}"
    chdir "scylla-rust-wrapper" do
      system "cargo", "build", "--profile", "packaging", "--verbose"
      system "./versioning.sh", "--profile", "packaging"
      lib.install Dir["target/packaging/*.dylib","target/packaging/*.a"]
    end

    cp "dist/common/pkgconfig/scylla-cpp-driver.pc.in", "scylla-cpp-driver.pc"
    inreplace "scylla-cpp-driver.pc" do |s|
      s.gsub! "@prefix@", "#{prefix}"
      s.gsub! "@exec_prefix@", "#{bin}"
      s.gsub! "@libdir@", "#{lib}"
      s.gsub! "@includedir@", "#{include}"
      s.gsub! "@version@", "#{version}"
    end

    cp "dist/common/pkgconfig/scylla-cpp-driver_static.pc.in", "scylla-cpp-driver_static.pc"
    inreplace "scylla-cpp-driver_static.pc" do |s|
      s.gsub! "@prefix@", "#{prefix}"
      s.gsub! "@exec_prefix@", "#{bin}"
      s.gsub! "@libdir@", "#{lib}"
      s.gsub! "@includedir@", "#{include}"
      s.gsub! "@version@", "#{version}"
    end

    (lib/"pkgconfig").install Dir["*.pc"]
    include.install "include/cassandra.h"
  end
end
