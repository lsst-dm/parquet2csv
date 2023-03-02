

cmake_minimum_required(VERSION 3.2.0)

project(globalProject)

add_subdirectory(reader)
add_subdirectory(listener)

install(TARGETS parquet_reader arrow_listener
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib)
