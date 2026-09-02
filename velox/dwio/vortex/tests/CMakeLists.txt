# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_library(velox_dwio_vortex_test_support VortexTestFile.cpp)
velox_add_test_headers(velox_dwio_vortex_test_support VortexTestFile.h)
target_link_libraries(
  velox_dwio_vortex_test_support
  velox_arrow_bridge
  velox_file
  velox_test_util
  velox_vector
  Vortex::vortex_velox
)

add_executable(velox_dwio_vortex_split_mapper_test VortexSplitMapperTest.cpp)
add_test(NAME velox_dwio_vortex_split_mapper_test COMMAND velox_dwio_vortex_split_mapper_test)
target_link_libraries(
  velox_dwio_vortex_split_mapper_test
  velox_dwio_vortex_reader
  velox_link_libs
  GTest::gtest
  GTest::gtest_main
  ${TEST_LINK_LIBS}
)

add_executable(velox_dwio_vortex_reader_test VortexReaderTest.cpp)
add_test(NAME velox_dwio_vortex_reader_test COMMAND velox_dwio_vortex_reader_test)
target_link_libraries(
  velox_dwio_vortex_reader_test
  velox_dwio_vortex_reader
  velox_dwio_vortex_test_support
  velox_vector_test_lib
  velox_link_libs
  GTest::gtest
  GTest::gtest_main
  ${TEST_LINK_LIBS}
)

add_executable(velox_dwio_vortex_filter_test VortexFilterTest.cpp)
add_test(NAME velox_dwio_vortex_filter_test COMMAND velox_dwio_vortex_filter_test)
target_link_libraries(
  velox_dwio_vortex_filter_test
  velox_dwio_vortex_reader
  velox_vector_test_lib
  velox_link_libs
  GTest::gtest
  GTest::gtest_main
  ${TEST_LINK_LIBS}
)

add_executable(velox_dwio_vortex_e2e_filter_test VortexE2EFilterTest.cpp)
add_test(NAME velox_dwio_vortex_e2e_filter_test COMMAND velox_dwio_vortex_e2e_filter_test)
target_link_libraries(
  velox_dwio_vortex_e2e_filter_test
  velox_dwio_vortex_reader
  velox_dwio_vortex_test_support
  velox_dwio_common_test_utils
  velox_vector_test_lib
  velox_link_libs
  GTest::gtest
  GTest::gtest_main
  ${TEST_LINK_LIBS}
)

if(VELOX_ENABLE_HIVE_CONNECTOR)
  add_executable(
    velox_dwio_vortex_table_scan_test
    VortexTableScanTest.cpp
    ${PROJECT_SOURCE_DIR}/velox/connectors/hive/iceberg/tests/IcebergTestBase.cpp
  )
  add_test(NAME velox_dwio_vortex_table_scan_test COMMAND velox_dwio_vortex_table_scan_test)
  target_link_libraries(
    velox_dwio_vortex_table_scan_test
    velox_dwio_vortex_reader
    velox_dwio_vortex_test_support
    velox_hive_connector
    velox_hive_iceberg_splitreader
    velox_hive_partition_function
    velox_dwio_common_exception
    velox_dwio_common_test_utils
    velox_vector_test_lib
    velox_vector_fuzzer
    velox_exec
    velox_exec_test_lib
    Folly::folly
    fmt::fmt
    GTest::gtest
    GTest::gtest_main
  )
endif()

if(VELOX_ENABLE_HIVE_CONNECTOR AND VELOX_ENABLE_TPCH_CONNECTOR)
  add_executable(velox_dwio_vortex_tpch_test VortexTpchTest.cpp)
  add_test(
    NAME velox_dwio_vortex_tpch_test
    COMMAND velox_dwio_vortex_tpch_test
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )
  target_link_libraries(
    velox_dwio_vortex_tpch_test
    velox_dwio_vortex_reader
    velox_dwio_vortex_test_support
    velox_hive_connector
    velox_tpch_connector
    velox_tpch_gen
    velox_exec_test_lib
    velox_functions_prestosql
    velox_aggregates
    velox_test_util
    velox_link_libs
    Folly::folly
    fmt::fmt
    GTest::gtest
  )
endif()

if(VELOX_ENABLE_BENCHMARKS AND VELOX_ENABLE_PARQUET AND VELOX_ENABLE_HIVE_CONNECTOR)
  add_executable(velox_dwio_vortex_scan_benchmark VortexParquetScanBenchmark.cpp)
  target_link_libraries(
    velox_dwio_vortex_scan_benchmark
    velox_dwio_vortex_reader
    velox_dwio_vortex_test_support
    velox_dwio_parquet_reader
    velox_dwio_parquet_writer
    velox_hive_connector
    velox_exec
    velox_exec_test_lib
    velox_vector_test_lib
    Folly::follybenchmark
  )
  if(VELOX_ENABLE_NIMBLE)
    target_compile_definitions(velox_dwio_vortex_scan_benchmark PRIVATE VELOX_ENABLE_NIMBLE)
    target_link_libraries(
      velox_dwio_vortex_scan_benchmark
      nimble_common_file_writer
      nimble_velox_selective
    )
  endif()
endif()
