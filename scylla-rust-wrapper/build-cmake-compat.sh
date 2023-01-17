#!/bin/bash
set -e

# The version number should be in the third line
# of Cargo.toml file (e.g. version = "0.0.1")
VERSION_NUMBER=$(sed -n '3p' < Cargo.toml)
REGEX="version = \"([0-9]+)\.([0-9]+)\.([0-9]+)\""
if [[ $VERSION_NUMBER =~ $REGEX ]]
then
  MAJOR=${BASH_REMATCH[1]}
  MINOR=${BASH_REMATCH[2]}
  PATCH=${BASH_REMATCH[3]}
else
  echo "Could not parse the version number in Cargo.toml"
  echo "Tried to parse a third line of Cargo.toml: $VERSION_NUMBER"
  echo 'but it did not contain a valid version number (e.g. version = "0.0.1").'
  exit 1
fi

create_symlinks() {
  rm -rf target/$1/cmake-compat/
  mkdir target/$1/cmake-compat/

  cp target/$1/libscylla_cpp_driver.so target/$1/cmake-compat/libscylla-cpp-driver.so.$MAJOR.$MINOR.$PATCH
  ln -s libscylla-cpp-driver.so.$MAJOR.$MINOR.$PATCH target/$1/cmake-compat/libscylla-cpp-driver.so.$MAJOR
  ln -s libscylla-cpp-driver.so.$MAJOR target/$1/cmake-compat/libscylla-cpp-driver.so
}

if [[ $1 == "DEBUG" ]]
then
  cargo build
  create_symlinks "debug"
elif [[ $1 == "RELEASE" ]]
then
  cargo build --release
  create_symlinks "release"
else
  echo 'Invalid build mode: $1 (only DEBUG or RELEASE permitted)'
  exit 1
fi
