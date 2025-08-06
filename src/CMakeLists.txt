include(CheckSymbolExists)
include(CheckCXXSourceCompiles)

file(GLOB SOURCES *.cpp *.hpp)

if(APPLE)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-unix.cpp ${CASS_SRC_DIR}/get_time-win.cpp)
elseif(UNIX)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-mac.cpp ${CASS_SRC_DIR}/get_time-win.cpp)
elseif(WIN32)
  list(REMOVE_ITEM SOURCES ${CASS_SRC_DIR}/get_time-mac.cpp ${CASS_SRC_DIR}/get_time-unix.cpp)
endif()

if(CASS_USE_OPENSSL)
  list(APPEND SOURCES ssl/ssl_openssl_impl.cpp ssl/ring_buffer_bio.cpp)
else()
  list(APPEND SOURCES ssl/ssl_no_impl.cpp)
endif()

if(CASS_USE_KERBEROS)
  list(APPEND INCLUDE_DIRS gssapi)
  list(APPEND SOURCES gssapi/dse_auth_gssapi.cpp gssapi/dse_auth_gssapi.hpp)
endif()

# Determine atomic library to include
if(CASS_USE_BOOST_ATOMIC)
  list(APPEND SOURCES atomic/atomic_boost.hpp)
elseif(CASS_USE_STD_ATOMIC)
  list(APPEND SOURCES atomic/atomic_std.hpp)
else()
  list(APPEND SOURCES atomic/atomic_intrinsics.hpp)
  if(WIN32)
    list(APPEND SOURCES atomic/atomic_intrinsics_msvc.hpp)
  else()
    list(APPEND SOURCES atomic/atomic_intrinsics_gcc.hpp)
  endif()
endif()

# Determine `optional` library to include
if(CMAKE_CXX_STANDARD LESS 17)
  message(STATUS "Using akrzemi's `optional` implementation")
  list(APPEND SOURCES optional/optional_akrzemi.hpp)
else()
  message(STATUS "Using std::optional library")
  list(APPEND SOURCES optional/optional_std.hpp)
endif()

# add_subdirectory(third_party/curl)
# add_subdirectory(third_party/hdr_histogram)
# add_subdirectory(third_party/http-parser)
# add_subdirectory(third_party/minizip)
add_subdirectory(third_party/sparsehash)

list(APPEND INCLUDE_DIRS 
  third_party/curl
  third_party/hdr_histogram
  third_party/http-parser
  third_party/minizip
  third_party/mt19937_64
  third_party/rapidjson/rapidjson
  third_party/sparsehash/src)

list(APPEND INCLUDE_DIRS ${CASS_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

#------------------------------
# Compiler flags
#------------------------------

# Ensure Windows platform is supported
if(WIN32)
  if(CMAKE_SYSTEM_VERSION GREATER 5.2 OR
      CMAKE_SYSTEM_VERSION EQUAL 5.2)
    add_definitions(-D_WIN32_WINNT=0x502)
  else()
    string(REPLACE "." "" WINDOWS_VERSION ${CMAKE_SYSTEM_VERSION})
    string(REGEX REPLACE "([0-9])" "0\\1" WINDOWS_VERSION ${WINDOWS_VERSION})
    message(FATAL_ERROR "Unable to build driver: Unsupported Windows platform 0x${WINDOWS_VERSION}")
  endif()
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wconversion -Wno-sign-conversion -Wno-shorten-64-to-32 -Wno-undefined-var-template -Werror")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-implicit-int-float-conversion")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU") # To many superfluous warnings generated with GCC when using -Wconversion (see: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=40752)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

#------------------------------
# Build configured header
#------------------------------

# Determine random availability
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  check_symbol_exists(GRND_NONBLOCK "linux/random.h" HAVE_GETRANDOM)
  if(CASS_USE_TIMERFD)
    check_symbol_exists(timerfd_create "sys/timerfd.h" HAVE_TIMERFD)
  endif()
else()
  check_symbol_exists(arc4random_buf "stdlib.h" HAVE_ARC4RANDOM)
endif()

# Determine if sigpipe is available
check_symbol_exists(SO_NOSIGPIPE "sys/socket.h;sys/types.h" HAVE_NOSIGPIPE)
check_symbol_exists(sigtimedwait "signal.h" HAVE_SIGTIMEDWAIT)
if (NOT WIN32 AND NOT HAVE_NOSIGPIPE AND NOT HAVE_SIGTIMEDWAIT)
  message(WARNING "Unable to handle SIGPIPE on your platform")
endif()

# Determine if hash is in the tr1 namespace
string(REPLACE "::" ";" HASH_NAMESPACE_LIST ${HASH_NAMESPACE})
foreach(NAMESPACE ${HASH_NAMESPACE_LIST})
  if(NAMESPACE STREQUAL "tr1")
    set(HASH_IN_TR1 1)
  endif()
endforeach()

# Check for GCC compiler builtins
if(NOT "${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  check_cxx_source_compiles("int main() { return __builtin_bswap32(42); }" HAVE_BUILTIN_BSWAP32)
  check_cxx_source_compiles("int main() { return __builtin_bswap64(42); }" HAVE_BUILTIN_BSWAP64)
endif()

set(HAVE_BOOST_ATOMIC ${CASS_USE_BOOST_ATOMIC})
set(HAVE_STD_ATOMIC ${CASS_USE_STD_ATOMIC})
set(HAVE_KERBEROS ${CASS_USE_KERBEROS})
set(HAVE_OPENSSL ${CASS_USE_OPENSSL})

# Generate the driver_config.hpp file
configure_file(
  ${CASS_ROOT_DIR}/driver_config.hpp.in 
  ${CMAKE_CURRENT_SOURCE_DIR}/driver_config.hpp)

