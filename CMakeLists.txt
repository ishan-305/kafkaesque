cmake_minimum_required(VERSION 3.20)
project(kafkaesque CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- pull GoogleTest automatically (no manual install) ---
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# --- streamlog library: all modules ---
add_library(streamlog
  src/common/crc32.cpp
  src/common/byte_buffer.cpp
  src/storage/record.cpp
  src/storage/index.cpp
  src/storage/segment.cpp
  src/storage/partition_log.cpp
  src/net/protocol.cpp
  src/net/tcp_server.cpp
  src/broker/offset_store.cpp
  src/broker/topic_manager.cpp
  src/broker/broker.cpp
  client/producer.cpp
  client/consumer.cpp
)
target_include_directories(streamlog PUBLIC include)
target_include_directories(streamlog PRIVATE client)
find_package(Threads REQUIRED)
target_link_libraries(streamlog PUBLIC Threads::Threads)

# --- broker binary ---
add_executable(kafkaesque_broker src/broker/main.cpp)
target_link_libraries(kafkaesque_broker PRIVATE streamlog)

# --- smoke client (milestone validation CLI) ---
add_executable(smoke_client tools/smoke_client.cpp)
target_link_libraries(smoke_client PRIVATE streamlog)

# --- tests: one executable per module ---
include(GoogleTest)
function(streamlog_test name)
  add_executable(${name} tests/${name}.cpp)
  target_link_libraries(${name} PRIVATE streamlog gtest_main)
  gtest_discover_tests(${name})
endfunction()

streamlog_test(common_test)
streamlog_test(record_test)
streamlog_test(index_test)
streamlog_test(segment_test)
streamlog_test(partition_log_test)
streamlog_test(protocol_test)
streamlog_test(tcp_server_test)
streamlog_test(integration_test)
