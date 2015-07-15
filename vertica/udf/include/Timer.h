/*
 * Timer.h
 *
 *  Created on: Oct 5, 2014
 *      Author: alekh
 */

#ifndef TIMER_H_
#define TIMER_H_

#include <time.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

using namespace std;


class Timer{
private:
	struct timespec start;
	struct timespec stop;
	double totalTime;

public:
	Timer(){
		totalTime = 0;
	}

	inline void current_utc_time(struct timespec *ts) {
		#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
		  clock_serv_t cclock;
		  mach_timespec_t mts;
		  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
		  clock_get_time(cclock, &mts);
		  mach_port_deallocate(mach_task_self(), cclock);
		  ts->tv_sec = mts.tv_sec;
		  ts->tv_nsec = mts.tv_nsec;
		#else
		  clock_gettime(CLOCK_REALTIME, ts);
		#endif
	}

	// TODO: move these to a separate class!
	void on(){
		current_utc_time(&start);
		//clock_gettime(CLOCK_REALTIME, &start);
	}

	void off(){
		current_utc_time(&stop);
		//clock_gettime(CLOCK_REALTIME, &stop);
		totalTime += ( (double)((stop.tv_sec * 1000000000) + stop.tv_nsec) - ((start.tv_sec * 1000000000) + start.tv_nsec) ) / 1000000.0;
	}

	double time(){
		return totalTime;
	}
};


#endif /* TIMER_H_ */
