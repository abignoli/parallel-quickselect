/*
 * parqselect.hpp
 *
 *      Author: Andrea Bignoli (andrea.bignoli@gmail.com)
 *
 *      Program used to test Parallel Quickselect algorithm. Runs tests of median computation
 *      and measure execution times, reporting the mean at the end.
 *
 *      Call format: ./parallel-quickset array_size number_of_tests [number_of_threads]
 *
 *      Parameters:
 *
 *      array_size : size of arrays to use for median computation
 *      number_of_tests : number of test runs to execute
 *      number_of_threads : If the number of threads is not specified, it will be automatically set to the maximum available.
 */

#include "stdio.h"
#include "omp.h"
#include "parqselect.hpp"
#include <iostream>
#include <string>
#include "stdlib.h"
#include "time.h"
#include <fstream>
#include <vector>

#define N 10000000
#define SPLIT 1000000
#define SPLIT_AMOUNTS 10

#define RAND_SPLIT 10000

#define TO_MILLISECONDS 1000

inline double get_time() {
	return omp_get_wtime();
}

template<typename T>
void cpy_array(T a[], T b[], unsigned long size) {
	for (unsigned long i = 0; i < size; i++)
		b[i] = a[i];
}

template<typename T>
T** create_2D_array(unsigned long nrows, unsigned long ncols) {
	T** ptr = new T*[nrows];  // allocate pointers
	T* pool = new T[nrows * ncols];  // allocate pool
	for (unsigned i = 0; i < nrows; ++i, pool += ncols)
		ptr[i] = pool;
	return ptr;
}

template<typename T>
void delete_2D_array(T** arr) {
	delete[] arr[0];  // remove the pool
	delete[] arr;     // remove the pointers
}

template<typename T>
void copy_2D_array(T** a, T** b, unsigned long nrows, unsigned long ncols) {
	for (unsigned long i = 0; i < nrows; i++)
		cpy_array(a[i], b[i], ncols);
}

using namespace std;

void put_randoms(int a[], unsigned long size) {
	for (unsigned long k = 0; k < size; k++)
		a[k] = rand();
}

double harmonic_avg(const vector<double> &v) {
	double sum_reps = 0;

	for(const auto e: v)
		sum_reps += 1/e;

	return v.size() / sum_reps;
}

int main(int argc, char *argv[]) {
	int *par_test_array;
	int rndit;
	unsigned long i, j, k, z;
	unsigned long size, size_split;
	srand(time(NULL));
	double start, end, elapsed;
	double avg_time, seq_avg_time, par_avg_time;
	vector<double> exec_times;
	unsigned long number_of_tests;
	int num_threads;

	ofstream outpar;

	if (argc < 3 || argc > 4) {
		printf("Call format: ./parallel-quickset array_size number_of_tests [number_of_threads]\n\nParameters:\n\n\tarray_size : size of arrays to use for median computation\n\tnumber_of_tests : number of test runs to execute\n\tnumber_of_threads : If the number of threads is not specified, it will be automatically set to the maximum available.");
		return 1;
	}

	size = std::stoul(argv[1], NULL, 10);
	number_of_tests = std::stoul(argv[2], NULL, 10);



	size_split = size / SPLIT_AMOUNTS;

	printf("Testing on arrays of size: %lu\n", size);

	if (argc == 4) {
		omp_set_dynamic(0);
		int chosen_amount_of_threads = std::stoul(argv[3], NULL, 10);
		omp_set_num_threads(chosen_amount_of_threads);
	}
	num_threads = omp_get_max_threads();

	printf("Testing Parallel Quickselect on %d threads...\n\n",num_threads);

	par_test_array = new int[size];

	avg_time = 0;

	for (j = 0; j < number_of_tests; j++) {
		printf("Running test %lu...\n", j);
		put_randoms(par_test_array,size);

		start = get_time();

		parallel_select_median(par_test_array, 0, size-1);

		end = get_time();

		elapsed = (end - start) * TO_MILLISECONDS;

		exec_times.push_back(elapsed);
		avg_time += (elapsed - avg_time) / (j + 1);

	}

	par_avg_time = avg_time;
	printf("\nExecution times\n\n");
	printf("Arithmetic mean: %f ms\n", avg_time);
	printf("Harmonic mean: %f ms\n", harmonic_avg(exec_times));

	delete[] par_test_array;

	return 0;
}

