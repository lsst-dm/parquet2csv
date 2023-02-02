

cmake_minimum_required(VERSION 3.2.0)

project(test_dp0)

#option(PARQUET_LINK_SHARED "Link to the Parquet shared library" ON)

# This ensures that things like -std=gnu++... get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(ARROW_HOME ="$ENV{ARROW_HOME}")

#add_subdirectory(src)
set(SOURCES src/main.cc src/partition_config.cc src/parquet_read_dp0.cc)
message("${SOURCES}")
add_executable(test ${SOURCES})

#set(Parquet_DIR $ENV{ARROW_HOME}/lib/cmake/Parquet)

#find_package(Parquet REQUIRED)

#if(PARQUET_LINK_SHARED)
#  target_link_libraries(parquet-arrow-example Parquet::parquet_shared)
#else()
#  target_link_libraries(parquet-arrow-example Parquet::parquet_static)
#endif()

target_include_directories(test PUBLIC $ENV{ARROW_HOME}/include src)
target_include_directories(test PUBLIC $ENV{ARROW_HOME}/../install_json/include)

target_link_directories(test PUBLIC $ENV{ARROW_HOME}/lib)
target_link_libraries(test arrow parquet)

