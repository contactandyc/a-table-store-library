# SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_table_store_library_sql-example LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

find_library(M_LIB m)

find_package(a_memory_library CONFIG REQUIRED)
find_package(a_bloom_filter_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(the_lz4_library CONFIG REQUIRED)
find_package(sql_parser_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(sql_example
  src/main.c
)

target_include_directories(sql_example PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(sql_example PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_table_store_library::a_table_store_library)
  find_package(a_table_store_library CONFIG REQUIRED)
endif()
# target_link_libraries(sql_example PRIVATE a_table_store_library::a_table_store_library)

target_link_libraries(sql_example PRIVATE
  a_memory_library::a_memory_library
  a_bloom_filter_library::a_bloom_filter_library
  the_macro_library::the_macro_library
  the_lz4_library::the_lz4_library
  sql_parser_library::sql_parser_library
  a_table_store_library::a_table_store_library
)

if(M_LIB)
  target_link_libraries(sql_example PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(sql_example PRIVATE /W4)
else()
  target_compile_options(sql_example PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS sql_example RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
