# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

# CMakeLists.txt for tests
cmake_minimum_required(VERSION 3.20)

project(a_table_store_library_tests LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

set(A_BUILD_VARIANT "debug" CACHE STRING
    "Variant to link via a_table_store_library::a_table_store_library (debug|memory|static|shared)")
set_property(CACHE A_BUILD_VARIANT PROPERTY STRINGS debug memory static shared)

option(A_ENABLE_COVERAGE "Enable code coverage instrumentation" OFF)

# ---- Determine compiler flags based on the variant ----
if(MSVC)
  set(_A_DEBUG_OPTS /Zi /Od)
  set(_A_RELEASE_OPTS /O2 /DNDEBUG)
else()
  set(_A_DEBUG_OPTS -O0 -g)
  set(_A_RELEASE_OPTS -O3 -DNDEBUG)
endif()

if(A_BUILD_VARIANT STREQUAL "debug" OR A_BUILD_VARIANT STREQUAL "memory")
  set(TEST_COMPILER_OPTS ${_A_DEBUG_OPTS})
else()
  set(TEST_COMPILER_OPTS ${_A_RELEASE_OPTS})
endif()

find_library(M_LIB m)

find_package(a_memory_library CONFIG REQUIRED)
find_package(a_bloom_filter_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)

# Fallback for standalone test builds (when not included via add_subdirectory)
if(NOT TARGET a_table_store_library::a_table_store_library)
  find_package(a_table_store_library CONFIG REQUIRED)
endif()

set(TEST_EXECUTABLES "")

# ---- Targets ----
# ==============================================================================
# test_lsm_cache Target (Standard Test)
# ==============================================================================
add_executable(test_lsm_cache
  src/test_lsm_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_arena.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_compaction.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_env.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_pool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/memtable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_builder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_reader.c
)

target_include_directories(test_lsm_cache BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_lsm_cache)

set_target_properties(test_lsm_cache PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_lsm_cache PRIVATE a_memory_library::a_memory_library)
target_link_libraries(test_lsm_cache PRIVATE a_bloom_filter_library::a_bloom_filter_library)
target_link_libraries(test_lsm_cache PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_lsm_cache PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_lsm_cache PRIVATE a_table_store_library::a_table_store_library)

if(M_LIB)
  target_link_libraries(test_lsm_cache PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_lsm_cache PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_lsm_cache PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_lsm_cache PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_lsm_cache PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_lsm_cache PRIVATE -O0 -g --coverage)
    target_link_options(test_lsm_cache PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_lsm_cache COMMAND $<TARGET_FILE:test_lsm_cache>)
# ==============================================================================
# test_lsm_db Target (Standard Test)
# ==============================================================================
add_executable(test_lsm_db
  src/test_lsm_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_arena.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_compaction.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_env.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_pool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/memtable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_builder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_reader.c
)

target_include_directories(test_lsm_db BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_lsm_db)

set_target_properties(test_lsm_db PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_lsm_db PRIVATE a_memory_library::a_memory_library)
target_link_libraries(test_lsm_db PRIVATE a_bloom_filter_library::a_bloom_filter_library)
target_link_libraries(test_lsm_db PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_lsm_db PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_lsm_db PRIVATE a_table_store_library::a_table_store_library)

if(M_LIB)
  target_link_libraries(test_lsm_db PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_lsm_db PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_lsm_db PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_lsm_db PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_lsm_db PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_lsm_db PRIVATE -O0 -g --coverage)
    target_link_options(test_lsm_db PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_lsm_db COMMAND $<TARGET_FILE:test_lsm_db>)
# ==============================================================================
# test_memtable Target (Standard Test)
# ==============================================================================
add_executable(test_memtable
  src/test_memtable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_arena.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_compaction.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_env.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_pool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/memtable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_builder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_reader.c
)

target_include_directories(test_memtable BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_memtable)

set_target_properties(test_memtable PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_memtable PRIVATE a_memory_library::a_memory_library)
target_link_libraries(test_memtable PRIVATE a_bloom_filter_library::a_bloom_filter_library)
target_link_libraries(test_memtable PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_memtable PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_memtable PRIVATE a_table_store_library::a_table_store_library)

if(M_LIB)
  target_link_libraries(test_memtable PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_memtable PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_memtable PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_memtable PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_memtable PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_memtable PRIVATE -O0 -g --coverage)
    target_link_options(test_memtable PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_memtable COMMAND $<TARGET_FILE:test_memtable>)
# ==============================================================================
# test_sstable Target (Standard Test)
# ==============================================================================
add_executable(test_sstable
  src/test_sstable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_arena.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_compaction.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_db.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_env.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_pool.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/lsm_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/memtable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_builder.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/sstable_reader.c
)

target_include_directories(test_sstable BEFORE PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

list(APPEND TEST_EXECUTABLES test_sstable)

set_target_properties(test_sstable PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

target_link_libraries(test_sstable PRIVATE a_memory_library::a_memory_library)
target_link_libraries(test_sstable PRIVATE a_bloom_filter_library::a_bloom_filter_library)
target_link_libraries(test_sstable PRIVATE the_macro_library::the_macro_library)
target_link_libraries(test_sstable PRIVATE the_lz4_library::the_lz4_library)
target_link_libraries(test_sstable PRIVATE a_table_store_library::a_table_store_library)

if(M_LIB)
  target_link_libraries(test_sstable PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(test_sstable PRIVATE /W4 ${TEST_COMPILER_OPTS})
else()
  target_compile_options(test_sstable PRIVATE -Wall -Wextra -Wpedantic ${TEST_COMPILER_OPTS})
endif()

if(A_ENABLE_COVERAGE)
  if (CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(test_sstable PRIVATE -O0 -g -fprofile-instr-generate -fcoverage-mapping)
    target_link_options(test_sstable PRIVATE -fprofile-instr-generate -fcoverage-mapping)
  elseif (CMAKE_C_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_sstable PRIVATE -O0 -g --coverage)
    target_link_options(test_sstable PRIVATE --coverage)
  endif()
endif()

add_test(NAME test_sstable COMMAND $<TARGET_FILE:test_sstable>)

enable_testing()

# ---- Coverage aggregation ----
add_custom_target(coverage_report COMMENT "Generate coverage report")

if(A_ENABLE_COVERAGE AND CMAKE_C_COMPILER_ID MATCHES "Clang")
  include(ProcessorCount)
  ProcessorCount(NPROC)
  if(NPROC EQUAL 0)
    set(NPROC 4)
  endif()

  add_custom_command(TARGET coverage_report PRE_BUILD
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -j ${NPROC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running tests with coverage instrumentation"
  )

  find_program(LLVM_PROFDATA_EXECUTABLE llvm-profdata)
  find_program(LLVM_COV_EXECUTABLE llvm-cov)
  if(LLVM_PROFDATA_EXECUTABLE AND LLVM_COV_EXECUTABLE)
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_PROFDATA_EXECUTABLE} merge -sparse *.profraw -o default.profdata
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Merging coverage data"
    )

    # Normal compiled library path
    add_custom_command(TARGET coverage_report POST_BUILD
      COMMAND ${LLVM_COV_EXECUTABLE} show
              ../liba_table_store_library_${A_BUILD_VARIANT}.a
              -instr-profile=default.profdata
              -format=html -output-dir=coverage_html
              --ignore-filename-regex='/usr/.*'
              --show-regions --show-line-counts-or-regions
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "Generated LLVM coverage report → open coverage_html/index.html"
    )
  endif()
endif()
