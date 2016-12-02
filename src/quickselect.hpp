/*
 * quickselect.hpp
 *
 *      Author: Andrea Bignoli (andrea.bignoli@gmail.com)
 *
 *      Standard Quickselect implementation
 */

#ifndef QUICKSELECT_HPP_
#define QUICKSELECT_HPP_

#include <stdlib.h>

// Select element at index_to_select in sorted a, using a variation of quicksort
template<typename T>
inline T partition(T a[], unsigned long low, unsigned long high) {
	// Select a random pivot
	//	NOTE: in case the array size is over MAX_INT (max value returned by int)
	//    the element will be randomly selected between 0 and MAX_INT
	int tmp;

	T pivot = a[low + rand() % (high - low + 1)];

	while (low < high) {
		while (a[low] < pivot)
			low++;

		while (a[high] > pivot)
			high--;

		if (a[low] == a[high])
			low++;
		else if (low < high) {
			tmp = a[low];
			a[low] = a[high];
			a[high] = tmp;
		}
	}

	return high;
}

template<typename T>
T quickselect(T a[], unsigned long low, unsigned long index_to_select,
		unsigned long high) {
	unsigned long pivot_position, smaller_than_pivot_count;

	if ( low == high )
		return a[low];
	pivot_position = partition(a, low, high);

	smaller_than_pivot_count = pivot_position - low;

	if (smaller_than_pivot_count == index_to_select)
		return a[pivot_position];
	else if (index_to_select < smaller_than_pivot_count)
		return quickselect(a, low, index_to_select, pivot_position - 1);
	else
		return quickselect(a, pivot_position, index_to_select - smaller_than_pivot_count, high);
}



template<typename T>
T select_median(T a[], unsigned long low, unsigned long high) {
	return quickselect(a, low,(high - low) / 2, high);
}


#endif /* QUICKSELECT_HPP_ */
