cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 14)

include_directories(.)

file(GLOB_RECURSE CoreSources src/*.cpp src/*.c)
add_library(LibMyfsCore STATIC ${CoreSources})
