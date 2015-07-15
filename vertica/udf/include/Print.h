/*
 * print.h
 *
 *  Created on: Oct 8, 2014
 *      Author: alekh
 */

#ifndef PRINT_H_
#define PRINT_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

typedef enum debug_level {INFO, WARNING, ERROR} DEBUG_LEVEL;
typedef enum print_level {STDOUT, FILEOUT} PRINT_LEVEL;


//#define DEBUG	// could be passing at compile time via -D option

#ifdef DEBUG
	#define debug(level, format, ...) _debug(level, format, __VA_ARGS__)
	#define print(format) _print(format)
	#define flush() _flush()
#else
	#define debug(format, ...)
	#define print(format)
	#define flush()
#endif



void _debug(DEBUG_LEVEL level, const char* format, ...) {
	va_list args;

	switch(level){
	case INFO:		fprintf(stdout, "[INFO] ");
					va_start(args, format);
					vfprintf(stdout, format, args);
					va_end(args);
					fprintf(stdout, "\n");
					break;

	case WARNING:	fprintf(stderr, "[WARNING] ");
					va_start(args, format);
					vfprintf(stderr, format, args);
					va_end(args);
					fprintf(stderr, "\n");
					break;

	case ERROR:		fprintf(stderr, "[ERROR] ");
					va_start(args, format);
					vfprintf(stderr, format, args);
					va_end(args);
					fprintf(stderr, "\n");
					exit(EXIT_FAILURE);
					break;
	}
}

void _print(const char* logString){
	printf(logString);
	printf("\n");
}


void _flush(){
	int i;
	for(i=0;i<100;i++)
		print("--------");
}



#endif /* PRINT_H_ */
