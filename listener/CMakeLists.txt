

cmake_minimum_required(VERSION 3.2.0)

project(arrow_listener)

# This ensures that things like -std=gnu++... get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

message(ARROW_HOME ="$ENV{ARROW_HOME}")

#add_subdirectory(src)
set(SOURCES 
    src/main.cc 
)
message("${SOURCES}")
add_executable(arrow_listener ${SOURCES})

target_include_directories(arrow_listener PUBLIC $ENV{ARROW_HOME}/include src)
target_include_directories(arrow_listener PUBLIC $ENV{ARROW_HOME}/../install_json/include)

target_link_directories(arrow_listener PUBLIC $ENV{ARROW_HOME}/lib)
target_link_libraries(arrow_listener arrow parquet)

