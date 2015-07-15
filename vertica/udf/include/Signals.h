/*
 * Signals.h
 *
 *  Created on: Nov 23, 2014
 *      Author: alekh
 */

#ifndef SIGNALS_H_
#define SIGNALS_H_

#include "Print.h"
#include <signal.h>
#include <execinfo.h>

void sigsegv_handler (int param){
	print("handling SIGSEGV");
	void *array[100];
	size_t size,j;

	// get void*'s for all entries on the stack
	size = backtrace(array, 100);

	// print out all the frames to stderr
	debug(INFO, "Error: signal %d:", param);
	char **strings = backtrace_symbols(array, size);
	for (j=0; j<size; j++)
		debug(INFO,"%s",strings[j]);

	flush();
	exit(1);
}

void handle_sigsegv(){
	signal(SIGSEGV, sigsegv_handler);
}


#endif /* SIGNALS_H_ */
