/*
 * Sorting.h
 *
 *  Created on: Oct 8, 2014
 *      Author: alekh
 */

#ifndef SORTING_H_
#define SORTING_H_


#include "Constants.h"
#include "Vertica.h"
#include "Print.h"

#include <map>
#include <set>

using namespace Vertica;
using namespace std;

struct message{
	vint key;
	vfloat value;
};
typedef struct message Message;

struct sort_message_store{
	Message *messages;
	int *roff, *woff;
	int *num_rmessages, *num_wmessages;
};
struct hash_message_store{
	multimap<vint,vfloat> *wmessages;
	multimap<vint,vfloat> *rmessages;
	multimap<vint,vfloat>::iterator *it;
};

//#define HASH_BASED
#ifdef SORT_BASED
	typedef struct sort_message_store MessageStore;
#elif defined(HASH_BASED)
	typedef struct hash_message_store MessageStore;
#else
	typedef struct sort_message_store MessageStore;
#endif


void radixsort(Message *c, int n, int shift);
void quicksort(Message *c, int n);

#ifdef RADIX_SORT
	#define doSort(m,n) radixsort(m,n,56)
#elif defined(QUICK_SORT)
	#define doSort(m,n) quicksort(m,n)
#else
	#define doSort(m,n) radixsort(m,n,56)
#endif



/**
 * Sort messages by their keys.
 */
void sortMessages(MessageStore **messageStores, int instance_id){
	#ifdef SORT_BASED
	int i;
	for(i=0;i<max_num_stores*num_data_nodes;i++){
		MessageStore ms =  messageStores[instance_id][i];
		doSort(&ms.messages[*ms.roff], *ms.num_rmessages);
	}
#endif
}

/**
 * Comparator for sorting the messages.
 */
int compareMessages(const void* p1, const void* p2){
	vint diff = ((Message*)p1)->key - ((Message*)p2)->key;
	if(diff > 0)
		return 1;
	else if(diff < 0)
		return -1;
	else
		return 0;
}

void quicksort(Message *c, int n){
	qsort(c, n, sizeof(Message), compareMessages);
}


// insertion sort
void insertion_sort(Message *c, int n) {
	int k;
	for (k = 1; k < n; k++) {
		Message key = c[k];
		int i = k-1;
		while ((i >= 0) && (key.key < c[i].key)) {
			c[i+1] = c[i];
			i--;
		}
		c[i+1] = key;
	}
}

// radix sort: switches to insertion sort after a threshold
void radixsort(Message *c, int n, int shift) {
	int last[256],ptr[256],cnt[256];
	int i,j,k,sorted,remain;
	Message temp,swap;

	memset(cnt, 0, 256*sizeof(unsigned int)); // Zero counters
	switch (shift) { // Count occurrences
	case 0: 	for(i=0;i<n;i++) cnt[c[i].key & 0xFF]++; break;
	case 8: 	for(i=0;i<n;i++) cnt[(c[i].key >> 8) & 0xFF]++; break;
	case 16: 	for(i=0;i<n;i++) cnt[(c[i].key >> 16) & 0xFF]++; break;
	case 24: 	for(i=0;i<n;i++) cnt[(c[i].key >> 24) & 0xFF]++; break;
	case 32: 	for(i=0;i<n;i++) cnt[(c[i].key >> 32) & 0xFF]++; break;
	case 40: 	for(i=0;i<n;i++) cnt[(c[i].key >> 40) & 0xFF]++; break;
	case 48: 	for(i=0;i<n;i++) cnt[(c[i].key >> 48) & 0xFF]++; break;
	case 56: 	for(i=0;i<n;i++) cnt[(c[i].key >> 56) & 0xFF]++; break;
	}
	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<256; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}
	if(!sorted){	// Go through all swaps
		i = 255;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.key >> shift) & 0xFF;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).key >> shift) & 0xFF;
					remain--;
				}while(i != k);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}
	if(shift > 0){	// Sort on next digit
		shift -= 8;
		for(i=0; i<256; i++){
			if (cnt[i] > 64)	// INSERT_SORT_LEVEL = 64
				radixsort(&c[last[i]-cnt[i]], cnt[i], shift);
			else if(cnt[i] > 1)
				insertion_sort(&c[last[i]-cnt[i]], cnt[i]);
		}
	}
}

void pointer_swap(multimap<vint,vfloat>*& a, multimap<vint,vfloat>*& b){
	multimap<vint,vfloat>* c = a;
	a = b;
	b = c;
}

void swapMessageStores(MessageStore **messageStores, int instance_id){
	int i;
	for(i=0;i<max_num_stores*num_data_nodes;i++){
#ifdef SORT_BASED
		MessageStore ms = messageStores[instance_id][i];
		int tmp = *ms.roff;
		*ms.roff = *ms.woff;
		*ms.woff = tmp;

		*ms.num_rmessages = *ms.num_wmessages;
		*ms.num_wmessages = 0;
#elif defined(HASH_BASED)
		pointer_swap(messageStores[instance_id][i].rmessages, messageStores[instance_id][i].wmessages);
		*messageStores[instance_id][i].it = messageStores[instance_id][i].rmessages->begin();
		messageStores[instance_id][i].wmessages->clear();
#endif
	}
}

long messageCount(MessageStore **messageStores){
	//print("counting messages"); //flush();
	long count=0; int i;
	for(i=0;i<max_num_stores*num_data_nodes;i++){
		if(messageStores[i]==NULL)
			continue;
		int j;
		for(j=0;j<max_num_stores*num_data_nodes;j++)
#ifdef SORT_BASED
			count += *messageStores[i][j].num_rmessages;
#elif defined(HASH_BASED)
			count += messageStores[i][j].rmessages->size();
#endif
	}
	//print("finished counting messages"); //flush();
	//debug(INFO,"%d messages",count);
	return count;
}

long messageCountBeforeSwap(MessageStore **messageStores, set<int> partitionIds){
	long count=0; int i;
	for(i=0;i<max_num_stores*num_data_nodes;i++){
		if(messageStores[i]==NULL || partitionIds.find(i)==partitionIds.end())
			continue;
		int j;
		for(j=0;j<max_num_stores*num_data_nodes;j++)
#ifdef SORT_BASED
			count += *messageStores[i][j].num_wmessages;
#elif defined(HASH_BASED)
			count += messageStores[i][j].wmessages->size();
#endif
	}
	return count;
}


#endif /* SORTING_H_ */
