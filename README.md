Parallel implementation of the QuickSelect Algorithm in C++, using OpenMP.

Author: Andrea Bignoli (andrea.bignoli@gmail.com)

************************************

BUILDING 

Example, using CMake:

git clone 
cd 
mkdir build
cd build
cmake ..
make

************************************

RUNNING

The included program is used to test Parallel Quickselect algorithm. Runs tests of median computation and measure execution times, reporting the mean at the end.

Call format: ./parallel-quickset array_size number_of_tests [number_of_threads]

Parameters:

    array_size : size of arrays to use for median computation
    number_of_tests : number of test runs to execute
    number_of_threads : If the number of threads is not specified, it will be automatically set to the maximum available.
