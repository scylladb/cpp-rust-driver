cmake_minimum_required(VERSION 3.17)
project(cppdriverv2 C)

# Enable ExternalProject CMake module
include(ExternalProject)

# Set default ExternalProject root directory
set_directory_properties(PROPERTIES EP_PREFIX ${CMAKE_BINARY_DIR}/Rust)

# Set a default build type if none was specified
set(default_build_type "Release")
if(EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE
      STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()

# Add rust_example as a CMake target
ExternalProject_Add(
    scylla-rust-driver
    DOWNLOAD_COMMAND ""
    CONFIGURE_COMMAND ""
    BUILD_COMMAND cargo build $<IF:$<CONFIG:Debug>,,--release>
    BINARY_DIR "${CMAKE_SOURCE_DIR}/scylla-rust-wrapper"
    INSTALL_COMMAND ""
    LOG_BUILD ON
    BUILD_ALWAYS TRUE)

add_library(scylla-cpp-driver SHARED IMPORTED)
set_property(TARGET scylla-cpp-driver APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(scylla-cpp-driver PROPERTIES
  IMPORTED_LOCATION_DEBUG "${CMAKE_SOURCE_DIR}/scylla-rust-wrapper/target/debug/libscylla_cpp_driver.so"
)
set_property(TARGET scylla-cpp-driver APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(scylla-cpp-driver PROPERTIES
  IMPORTED_LOCATION_RELEASE "${CMAKE_SOURCE_DIR}/scylla-rust-wrapper/target/release/libscylla_cpp_driver.so"
)
set_target_properties(scylla-cpp-driver PROPERTIES
  MAP_IMPORTED_CONFIG_MINSIZEREL Release
  MAP_IMPORTED_CONFIG_RELWITHDEBINFO Release
)
add_dependencies(scylla-cpp-driver scylla-rust-driver)
target_include_directories(scylla-cpp-driver INTERFACE "${CMAKE_SOURCE_DIR}/include")

add_executable(main src/main.c)
target_link_libraries(main scylla-cpp-driver)

