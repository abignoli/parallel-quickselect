cmake_minimum_required(VERSION 2.6)
project (parallelQuickSelect CXX)

set(GCC_COMPILER_FLAGS "-std=c++11 -fpermissive -fopenmp")
set(GCC_LINKER_FLAGS "-std=c++11 -fpermissive -fopenmp")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COMPILER_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${GCC_LINKER_FLAGS}")

file(GLOB_RECURSE ParallelQuickSelect_SOURCE 
                  src/*.cpp
                  )
add_executable(parallelQuickSelect ${ParallelQuickSelect_SOURCE})