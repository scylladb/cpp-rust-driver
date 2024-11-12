require "formula"

class ScyllaCppRustDriver < Formula
  homepage "https://github.com/scylladb/cpp-rust-driver"
  head "https://github.com/scylladb/cpp-rust-driver.git", branch: "master"

  depends_on "cmake" => :build
  depends_on "rust" => :build
  depends_on "openssl@3"

  def install
    system "cmake", "-S", ".", "-B", "build", "-DCMAKE_BUILD_TYPE=RelWithDebInfo", "-DCMAKE_VERBOSE_MAKEFILE=ON", *std_cmake_args
    system "cmake", "--build", "build"
    system "cmake", "--install", "build"
  end
end
