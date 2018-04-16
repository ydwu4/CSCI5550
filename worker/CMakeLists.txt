cmake_minimum_required(VERSION 3.5)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(../core)

set(LibMyfs LibMyfsCore)

add_executable(worker worker.cpp)
target_link_libraries(worker ${LibMyfs} ${ExtLibs})
