

cmake_minimum_required(VERSION 3.2.0)

project(parquet_reader)

#option(PARQUET_LINK_SHARED "Link to the Parquet shared library" ON)

# This ensures that things like -std=gnu++... get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#add_subdirectory(src)
set(SOURCES 
    src/main.cc 
    src/partition_config.cc 
    src/parquet_read_file.cc 
    src/streaming_socket.cc 
    src/csv_dump_file.cc 
    src/ipc_dump_file.cc)

add_executable(parquet_reader ${SOURCES})

#target_include_directories(parquet_reader PUBLIC $ENV{ARROW_HOME}/include src)
#target_include_directories(parquet_reader PUBLIC $ENV{ARROW_HOME}/../install_json/include)

#target_link_directories(parquet_reader PUBLIC $ENV{ARROW_HOME}/lib)
target_link_libraries(parquet_reader arrow parquet)


