cmake_minimum_required(VERSION 3.14 FATAL_ERROR)
project(skysync LANGUAGES C CXX)

enable_language(C CXX ASM)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED on)
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
# -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -pg")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -pthread -mcrc32 -msse4")

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(BuildType)
message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")

find_package(POPT)
find_package (ZLIB)
find_package(LIBB2)

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_library(ISAL isal REQUIRED)
find_library(ISAL_crypto isal_crypto REQUIRED)
# find_library(IPSec_MB IPSec_MB REQUIRED)
find_library(PTHREAD pthread REQUIRED)
find_library(LIBRSYNC rsync REQUIRED)
find_package(mimalloc 2.1 REQUIRED)
find_library(LIBEV ev)
find_library(LIBEVENT event)
find_library(LIBEVENT2 event2)
find_library(LIBEVENT_OPENSSL event_openssl)
find_library(LIBLEVELDB leveldb)
find_library(LIBPROTOC protoc)

find_package(GFLAGS REQUIRED)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_library(LIBURING uring REQUIRED)
find_package(TBB REQUIRED)

# check where the library is installed
message(STATUS "ISAL = ${ISAL}")
message(STATUS "ISAL_crypto = ${ISAL_crypto}")
# message(STATUS "IPSec_MB = ${IPSec_MB}")
message(STATUS "LIBRSYNC = ${LIBRSYNC}")
message(STATUS "LIBEV = ${LIBEV}")
message(STATUS "LIBEVENT_OPENSSL = ${LIBEVENT_OPENSSL}")
message(STATUS "LIBEVENT = ${LIBEVENT}")
message(STATUS "LIBLEVELDB = ${LIBLEVELDB}")
message(STATUS "LIBPROTOC = ${LIBPROTOC}")
message(STATUS "LIBURING = ${LIBURING}")

include_directories(thirdparty/brpc/output/include)
link_directories(thirdparty/brpc/output/lib)

set(PHOTON_ENABLE_URING ON CACHE INTERNAL "Enable iouring")
set(PHOTON_CXX_STANDARD 14 CACHE INTERNAL "C++ standard")
add_subdirectory(thirdparty/photon)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/crc32)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/thread)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/metadata)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/dsync)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/skysync-f)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/skysync-c)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tests)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/b3)
set(blake3_SOURCES)
list(APPEND blake3_SOURCES
    src/b3/blake3.c
    src/b3/blake3_dispatch.c
    src/b3/blake3_portable.c
    src/b3/blake3_sse2_x86-64_unix.S
    src/b3/blake3_sse41_x86-64_unix.S
    src/b3/blake3_avx2_x86-64_unix.S
    src/b3/blake3_avx512_x86-64_unix.S
    )

add_executable(rsync_test
               tests/rsync/rsync_test.cpp
               src/tools.cpp
               tests/rsync/rsync_http.c
              )
target_link_libraries(rsync_test ${blake2_LIBS} ${POPT_LIBRARIES} ${LIBRSYNC})

add_executable(crc32c_test
               tests/crc32c_test.cpp
               src/crc32/crc32.c
               src/tools.cpp
               src/crc32/crc32c.c
               src/simd_checksum_avx2.cpp
               src/simd_checksum_avx2.S
               )
target_compile_options(crc32c_test PRIVATE -mcrc32 -msse4 -mssse3)
target_link_libraries(crc32c_test ${ZLIB_LIBRARIES} ${ISAL} ${ISAL_crypto})

add_executable(crc32_rolling_test
              tests/crc32_rolling_test.c
              src/crc32/crc32.c
              src/crc32/crc32c.c
               )
target_link_libraries(crc32_rolling_test ${ZLIB_LIBRARIES} ${ISAL})

add_executable(sha256_speed_test
               tests/sha256_speed_test.cpp
               ${blake3_SOURCES}
               )
target_compile_options(sha256_speed_test PRIVATE -msse4 -msha)
target_link_libraries(sha256_speed_test ${ZLIB_LIBRARIES} ${ISAL} ${ISAL_crypto} OpenSSL::SSL OpenSSL::Crypto)

add_executable(file_checksum_test
               tests/file_checksum_test.cpp
               src/crc32/crc32.c
               src/tools.cpp
               src/crc32/crc32c.c
               src/adler_rollsum.c
               ${blake3_SOURCES}
               )
target_compile_options(file_checksum_test PRIVATE -mcrc32 -msse4 -mssse3 -msha)
target_link_libraries(file_checksum_test ${ZLIB_LIBRARIES} ${ISAL} ${ISAL_crypto} OpenSSL::SSL OpenSSL::Crypto)

add_executable(fs_verity_test
               tests/metadata/fs_verity_test.cpp
               src/metadata/fs_verity.cpp
               src/tools.cpp
               )
# target_link_libraries(fs_verity_test)

add_executable(mega_test
               tests/metadata/mega_test.cpp
               src/crc32/crc32c.c
               )
target_compile_options(mega_test PRIVATE -mcrc32 -msse4 -mssse3 -msha)
target_link_libraries(mega_test ${ZLIB_LIBRARIES} ${ISAL} ${ISAL_crypto} OpenSSL::SSL OpenSSL::Crypto)

add_executable(rsync_http_client
               src/tools.cpp
               tests/rsync/rsync_http_client.cpp
               tests/rsync/rsync_http.c
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
               )
target_link_libraries(rsync_http_client ${LIBRSYNC} ${PTHREAD} ${LIBURING} photon_shared gflags)

add_executable(rsync_http_server
               src/tools.cpp
               tests/rsync/rsync_http_server.cpp
               tests/rsync/rsync_http.c
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
               )
target_link_libraries(rsync_http_server ${LIBRSYNC} ${PTHREAD} ${LIBURING} photon_shared gflags)

add_executable(dsync_test
               tests/dsync/dsync_test.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(dsync_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(dsync_http_server
               src/dsync/dsync_http_server.cpp
               src/dsync/dsync.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
                src/dsync/dsync.pb.cc
               )
target_link_libraries(dsync_http_server ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags protobuf)

add_executable(dsync_http_client
               src/dsync/dsync_http_client.cpp
               src/dsync/dsync.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
                src/dsync/dsync.pb.cc
               )
target_link_libraries(dsync_http_client ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags protobuf)

add_executable(rolling_fsc_test
               tests/skysync-f/rolling_fsc_test.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(rolling_fsc_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(skysync_f_test
               tests/skysync-f/skysync_f_test.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(skysync_f_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(skysync_f_http_server
               src/skysync-f/skysync_f_http_server.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/dsync/dsync.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
               thirdparty/photon/net/http/body.cpp
               thirdparty/photon/net/http/url.cpp
               src/skysync-f/skysync.pb.cc
               )
target_link_libraries(skysync_f_http_server ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags ${LIBPROTOC} protobuf)

add_executable(skysync_f_http_client
               src/skysync-f/skysync_f_http_client.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/dsync/dsync.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
               thirdparty/photon/net/http/body.cpp
               thirdparty/photon/net/http/url.cpp
               src/skysync-f/skysync.pb.cc
               )
target_link_libraries(skysync_f_http_client ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags ${LIBPROTOC} protobuf)

add_executable(skysync_c_test
               tests/skysync-c/skysync_c_test.cpp
               src/skysync-c/skysync_c_worker.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(skysync_c_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(bounds_check_test
               tests/skysync-c/bounds_check_test.cpp
               src/skysync-c/skysync_c_worker.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(bounds_check_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(serial_cdc_test
               tests/skysync-c/serial_cdc_test.cpp
               src/skysync-c/skysync_c_worker.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               )
target_link_libraries(serial_cdc_test ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb)

add_executable(skysync_c_http_server
               src/skysync-c/skysync_c_http_server.cpp
               src/skysync-c/skysync_c_worker.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
                src/dsync/dsync.pb.cc
               )
target_link_libraries(skysync_c_http_server ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags protobuf)

add_executable(skysync_c_http_client
               src/skysync-c/skysync_c_http_client.cpp
               src/skysync-c/skysync_c_worker.cpp
               src/skysync-f/skysync_f_worker.cpp
               src/skysync-f/fsc_hash.cpp
               src/skysync_c.cpp
               src/crc32/crc32.c
               src/crc32/crc32c.c
               src/tools.cpp
               src/fastcdc.cpp
               src/ring_buffer.c
               src/dsync/dsync.cpp
               src/linked_hash.cpp
               ${blake3_SOURCES}
               thirdparty/photon/net/http/client.cpp
               thirdparty/photon/net/http/headers.cpp
               thirdparty/photon/net/http/message.cpp
                thirdparty/photon/net/http/body.cpp
                thirdparty/photon/net/http/url.cpp
                src/dsync/dsync.pb.cc
               )
target_link_libraries(skysync_c_http_client ${ZLIB_LIBRARIES} OpenSSL::Crypto ${ISAL} ${ISAL_crypto} ${PTHREAD} photon_shared mimalloc TBB::tbb ${LIBURING} gflags protobuf)