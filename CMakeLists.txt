cmake_minimum_required(VERSION 3.5)
project(myfs)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

if (DEFINED ENV{BOOST_ROOT})
  set(BOOST_ROOT $ENV{BOOST_ROOT})
endif ()
find_package(Boost REQUIRED COMPONENTS iostreams system thread)
include_directories(${Boost_INCLUDE_DIRS})
set(ExtLibs ${ExtLibs} ${Boost_LIBRARIES})

if (DEFINED ENV{LIBFUSE_ROOT})
  set(LIBFUSE_ROOT $ENV{LIBFUSE_ROOT})
endif ()
find_package(LIBFUSE REQUIRED)
include_directories(${Libfuse_INCLUDE_DIR})
add_definitions(${Libfuse_DEFINITIONS})
set(ExtLibs ${ExtLibs} ${Libfuse_LIBRARY})

# for zeromq
include_directories("${CMAKE_SOURCE_DIR}/third_party/zeromq/include")
set(ExtLibs ${ExtLibs} "${CMAKE_SOURCE_DIR}/third_party/zeromq/lib/libzmq.so")

# for cppzmq
include_directories("${CMAKE_SOURCE_DIR}/third_party/cppzmq/include")

add_subdirectory(core)
add_subdirectory(master)
add_subdirectory(worker)
