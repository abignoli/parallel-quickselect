/*
 * parqselect.hpp
 *
 *      Author: Andrea Bignoli (andrea.bignoli@gmail.com)
 *
 *      Parallel Quickselect implementation
 */

#ifndef PARQSELECT_HPP_
#define PARQSELECT_HPP_

#include <stdlib.h>
#include "omp.h"
#include "quickselect.hpp"


// Select element at index_to_select in sorted a, using a variation of quicksort
template<typename T>
inline T partition(T a[], unsigned long low, unsigned long high, T pivot) {
	int tmp;
	const unsigned long init_high=high;

	/*
	 * 4 Cases:
	 *
	 * 1 - pivot smaller than any element in the array: return low
	 * 2 - pivot higher than any element in the array: return high+1
	 * 3 - pivot value in the array: return rightmost occurrence of pivot value
	 * 4 - pivot value not in the array, but between max and min values in the array: return i | a[i] > pivot and a[j] < pivot for each j < i
	 */

	while (low < high) {
		while (a[low] < pivot && low < high)
			low++;

		if(low==high)
			break;

		while (a[high] > pivot && high > low)
			high--;

		if(low==high)
			break;

		if (a[low] == a[high]) // Both are equal to pivot
			low++;
		else if (low < high) {
			tmp = a[low];
			a[low] = a[high];
			a[high] = tmp;
		}
	}

	// Address Case 2
	if(high==init_high && a[high] < pivot) {
		high++;
		// high_moved = true;
	}

	return high;
}

typedef struct bounds {
	unsigned long low;
	unsigned long high;
} bounds;

#define ERROR_SIGNAL -1

template<typename T>
T parallel_quickselect_no_alloc(T a[], unsigned long low, unsigned long index_to_select,
		unsigned long high, const int num_threads, bounds t_bounds[], unsigned long p_indexes[]) {
	unsigned long pivot_position, smaller_than_pivot_count;
	T pivot;
	int i;
	int j,j_size;
	int k;
	unsigned long size = high-low+1;
	int pivot_index_on_current_partitions;
	int pivot_index;
	int accum;
	int cur_index;
	int t_max_size, max_size;
	int non_zero_count;
	T all_partitions_size_one_fallback[num_threads];
	const unsigned long starting_low = low;

	if(index_to_select >= size)
		// Error
		return ERROR_SIGNAL;

	if(num_threads*2>=size)
		// Not worth using threads
		return quickselect(a, low, index_to_select, high);

	// Initialize partitions by splitting array evenly among threads
	for(i=0;i<num_threads;i++) {
		t_bounds[i].low = low + i * size / num_threads;
		t_bounds[i].high = i < num_threads - 1 ? low + (i+1) * size / num_threads - 1 : high;
	}

	while(true) {

		// Compute pivot
		pivot_index_on_current_partitions = rand() % size;
		for(i=0,pivot_index=0;pivot_index_on_current_partitions >= 0; i++) 
			pivot_index_on_current_partitions -= t_bounds[i].high - t_bounds[i].low + 1;
		
		i--;
		pivot_index_on_current_partitions += t_bounds[i].high - t_bounds[i].low + 1;
		pivot_index = t_bounds[i].low + pivot_index_on_current_partitions;
		pivot = a[pivot_index];

		// Parallel partitioning

#pragma omp parallel for schedule(static,1)
		for(i=0;i<num_threads;i++)
			// Partition of size greater than 1
			p_indexes[i]=partition(a, t_bounds[i].low, t_bounds[i].high, pivot); // If pivot bigger than any element, returns high+1


		// Integrate results to discover the number of elements smaller than chosen pivot
		smaller_than_pivot_count = 0;
		for(i=0;i<num_threads;i++)
			/*
			 * a[p_indexes[i]] == pivot accounts for case 1,3
			 * p_indexes[i] <= t_bounds[i].high accounts for case 2
			 * if the conditions hold we are in case 4 and it's safe to add 1 to the size
			 *
			 * case 1: 0
			 * case 2: size
			 */
			smaller_than_pivot_count+=p_indexes[i]-t_bounds[i].low;

		if(smaller_than_pivot_count == index_to_select)
			break;

		// Update size according to whether index to select is in the left or right portions

		if(index_to_select < smaller_than_pivot_count) {
			// Pivot is in the left portions

			// Update lower and upper partition bounds for each thread
			for(i=0;i<num_threads;i++) {
				/*
				 * Case 1: If pivot was smaller than any element in the partition high = low - 1 now
				 * Case 2: If pivot was higher than any element in the partition high is unchanged
				 */
				if(p_indexes[i] != 0)
					t_bounds[i].high = p_indexes[i] - 1;
				else
/*					 If p_indexes is 0, then the partition will be surely zero-sized (since low >= 0)
					 Since high is unsigned, we cannot put -1 in it. This is relevant when low=0.
					 High should be low - 1, but high can't be -1.

					 This workaround is the equivalent as setting the partition to size 0,
					 achieving the desired result for size computation and zero-size partition fix*/
					t_bounds[i].low = t_bounds[i].high+1;
			}

		} else {
			// Pivot is in the right portions

			/*
			 * Case 1: If pivot was smaller than any element in the partition low is unchanged
			 * Case 2: If pivot was higher than any element in the partition low = high + 1 now
			 */
			 
			// Update lower and upper partition bounds for each thread
			for(i=0;i<num_threads;i++)
				t_bounds[i].low = p_indexes[i];
		}

		// Compute size
		size = 0;
		for(i=0;i<num_threads;i++) 
			if(t_bounds[i].high >= t_bounds[i].low)
				size += t_bounds[i].high - t_bounds[i].low + 1;
		

		if(index_to_select > smaller_than_pivot_count)
			index_to_select -= smaller_than_pivot_count;

		// if size is too small fallback to standard quickselect
		if(num_threads>=size) {
			// Collect values in fallback array
			for(j=0,k=0; j < num_threads; j++) {
				for(i= t_bounds[j].low; i <= t_bounds[j].high; i++) {
					all_partitions_size_one_fallback[k] = a[i];
					k++;
				}
			}
			return quickselect(all_partitions_size_one_fallback, 0, index_to_select, size-1);
		}

		// Fix partitions of size zero
		for(i=0;i<num_threads;i++)
			if(t_bounds[i].high < t_bounds[i].low) {
				// Thread i is assigned a partition of size 0

				// Look for thread t_max_size with partition of maximum size
				max_size=0;
				t_max_size=i;
				non_zero_count=0;
				for(j=0; j < num_threads; j++) {
					// Finding biggest partition j to split with i
					if(t_bounds[j].high > t_bounds[j].low) {
						j_size = t_bounds[j].high - t_bounds[j].low + 1;
						if(max_size<j_size) {
							max_size = j_size;
							t_max_size = j;
						}
					}
				}

				// Note: since size > num_threads and thread i has size 0 we can guarantee that there is at least one thread with size > 1, so t_max_size > 1

				j=t_max_size;
				if(i<j) {
					// i gets left half from j
					t_bounds[i].low = t_bounds[j].low;
					t_bounds[i].high = (t_bounds[j].high + t_bounds[j].low) / 2;
					t_bounds[j].low = t_bounds[i].high+1;
				} else {
					// i gets right half from j
					t_bounds[i].high = t_bounds[j].high;
					t_bounds[j].high = (t_bounds[j].high + t_bounds[j].low) / 2;
					t_bounds[i].low = t_bounds[j].high+1;
				}
			}
	}

	return pivot;
}

template<typename T>
T parallel_quickselect(T a[], unsigned long low, unsigned long index_to_select,
		unsigned long high) {
	int num_threads = omp_get_max_threads();
	bounds *t_bounds;
	unsigned long *p_indexes;
	T result;

	t_bounds=new bounds[num_threads];
	p_indexes = new unsigned long[num_threads];

	result = parallel_quickselect_no_alloc(a, low, index_to_select,
			high, num_threads, t_bounds,p_indexes);

	delete[] t_bounds;
	delete[] p_indexes;

	return result;
}

template<typename T>
T parallel_select_median(T a[], unsigned long low, unsigned long high) {
	return parallel_quickselect(a, low,(high - low) / 2, high);
}

template<typename T>
T parallel_select_median_no_alloc(T a[], unsigned long low, unsigned long high, const int num_threads, bounds t_bounds[], unsigned long p_indexes[]) {
	return parallel_quickselect_no_alloc(a, low,(high - low) / 2, high, num_threads, t_bounds, p_indexes);
}


#endif /* PARALLEL_QUICKSELECT_HPP_ */
