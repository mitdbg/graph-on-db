/* Copyright (c) 2005 - 2012, Hewlett-Packard Development Co., L.P.  -*- C++ -*- 
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the Hewlett-Packard Company nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ****************************/
/* 
 * Description: Support code for UDx subsystem
 *
 * Create Date: Mar 7, 2011
 */
#ifndef TIMESTAMP_UDX_SHARED_H
#define TIMESTAMP_UDX_SHARED_H

#include <stdio.h>

#ifndef kDiv
// On most machines, n/m and n%m return negative numbers when n is negative
// This is a problem in many cases, e.g., timestamps and intervals
// Compute n divided_by m with positive remainder, even if n is negative
#define kDiv(_n, _m) (_n < 0 ? ~(~(_n) / (_m)) : (_n) / (_m))
// Compute n mod m with positive remainder (per Knuth), even if n is negative
#define kMod(_n, _m) (_n < 0 ? ~(~(_n) % (_m)) + (_m) : (_n) % (_m))
#endif

/**
* \internal
* \file TimestampUDxShared.h
* \brief Utilities for handling timestamps and dates
*/

/*
 * Fields for time decoding.
 *
 * Can't have more of these than there are bits in an unsigned int
 * since these are turned into bit masks during parsing and decoding.
 *
 * Furthermore, the values for YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
 * must be in the range 0..14 so that the associated bitmasks can fit
 * into the left half of an INTERVAL's typmod value.
 */

typedef enum {
    RESERV = 0,
    MONTH  = 1,
    YEAR   = 2,
    DAY    = 3,
    NEG_INTERVAL = 4,
    TZ     = 5,
    DTZ    = 6,
    DTZMOD = 7,
    IGNORE_DTF = 8,
    AMPM   = 9,
    HOUR   = 10,
    MINUTE = 11,
    SECOND = 12,
    DOY    = 13,
    DOW    = 14,
    UNITS  = 15,
    ADBC   = 16,
/* these are only for relative dates */
    AGO    = 17,
/* generic fields to help with parsing */
    ISODATE = 20,
    ISOTIME = 21,
/* reserved for unrecognized string values */
    UNKNOWN_FIELD = 31
} DTtype;

#define TIMESTAMP_MASK(b) (1 << (b))
#define INTERVAL_MASK(b) (1 << (b))

#define DT_NOBEGIN  (-INT64CONST(0x7fffffffffffffff))  // was == vint_null
#define DT_NOEND    (INT64CONST(0x7fffffffffffffff))
#define DT_NULL     0x8000000000000000LL // vint_null

#define TIME_T_TO_TIMESTAMPTZ(s,us) (((s) - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay) * usPerSecond + (us))

#define TIMESTAMP_NOBEGIN(j)    do {j = DT_NOBEGIN;} while (0)
#define TIMESTAMP_IS_NOBEGIN(j) ((j) == DT_NOBEGIN)

#define TIMESTAMP_NOEND(j)      do {j = DT_NOEND;} while (0)
#define TIMESTAMP_IS_NOEND(j)   ((j) == DT_NOEND)

#define TIMESTAMP_NOT_FINITE(j) (TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j))

/* Macros to handle packing and unpacking the typmod field for intervals */
#define INTERVAL_YEAR2MONTH (INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH))
#define INTERVAL_DAY2SECOND (INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | \
                             INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND))
#define INTERVAL_RANGE_MASK (INTERVAL_YEAR2MONTH | INTERVAL_DAY2SECOND)
#define INTERVAL_PRECISION_MASK (0xFFFF)
#define INTERVAL_PRECISION(t) ((t) & INTERVAL_PRECISION_MASK)
#define INTERVAL_RANGE(t) (((t) >> 16) & INTERVAL_RANGE_MASK)
#define INTERVAL_TYPMOD(p,r) (((r) << 16) | INTERVAL_PRECISION(p))

#define Day0        INT64CONST(-63113904000000000) // Jan 1, 0000

#define MAX_SECONDS_PRECISION 6         // for time/tz, timestamp/tz, interval
#define MAX_LEADING_PRECISION 19        // interval -- enforced, but not obeyed

// useful constants
#define usPerSecond INT64CONST(1000000)
#define usPerMinute (usPerSecond * 60)  //     60000000
#define usPerHour   (usPerMinute * 60)  //   3600000000
#define usPerDay    (usPerHour * 24)    //  86400000000
#define usPerWeek   (usPerDay * 7)      // 604800000000
#define usPerMonth  (usPerDay * 30)     // often wrong
#define usPerYear   (usPerDay * 365)    // sometimes wrong
#define usPerAnnum  (usPerDay * 365.25) // exact Julian year
#define POSTGRES_EPOCH_JDATE 2451545
#define UNIX_EPOCH_JDATE 2440588

#define SECS_PER_MINUTE 60
#define MINS_PER_HOUR   60
#define HOURS_PER_DAY   24
#define MONTHS_PER_YEAR  12

#define secPerHour  (60 * 60)           //  3600
#define secPerDay   (secPerHour * 24)   // 86400

#define minPerDay   (60 * 24)           // 1440

/** Represents absolute time and date.
 *
 * The value in Timestamp is the number of microseconds since
 * 2000-01-01 00:00:00 with no timezone implied
 *
 */
typedef int64 Timestamp;

/** Represents absolute time and date with time zone.
 *
 * The value in TimestampTz is the number of microseconds since
 * 2000-01-01 00:00:00 GMT
 */
typedef int64 TimestampTz;

/** Represents delta time.
 *
 * The value in Interval is number of microseconds, which is limited to
 * +/- 2^63-1 microseconds = 106751991 days 4 hours 54.775807 seconds
 * or +/-9223372036854775807 months = 768614336404564650 years 7 months
 *
 * Interval can be directly added/subtracted from Timestamp/TimestampTz to
 * have a meaningful result
 */
typedef int64 Interval;

/** Represents delta time.
 *
 * The value in IntervalYM is number of months
 *
 * So Interval can be directly added/subtracted from Timestamp/TimestampTz to
 * have a meaningful result, but IntervalYM cannot
 */
typedef int64 IntervalYM;

/**
 * The value in DateADT is the number of days
 */
typedef int64 DateADT;

/**
 * TimeADT represents time within a day
 *
 * The value in TimeADT number of microseconds within 24 hours
 */
typedef int64 TimeADT;

/** Represents time within a day in a timezone
 *
 * The value in TimeADT consists of 2 parts:
 *
 * 1. The lower 24 bits (defined as ZoneFieldWidth) contains the timezone plus
 *    24 hours, specified in seconds
 *    SQL-2008 limits the timezone itself to range between +/-14 hours
 *
 * 2. The rest of higher bits contains the time in GMT, value specified as
 *    number of microseconds within 24 hours
 */
typedef int64 TimeTzADT;



// Time part is stored as signed GMT to make sorting easy (PG uses local time)
//   This is a bug, time part should be stored as GMT mod 24-hours. 
// Zone goes logically from -24 hours to +24 hours, specified in seconds, but
// it is stored as 0 to 48 hours (i.e., plus 24 hours) to sort correctly.
//   This is also a bug, it should be stored as (24-hours - zone) to sort right.
// SQL-2008 limits this range to +/-14 hours, and we enforce that.
#define ZoneFieldWidth 24
#define ZoneMask ((1LU << ZoneFieldWidth) - 1)

#define MaxTimezone  (14 * secPerHour)            // SQL-2008 limit
// allow one leap second for TIME, but TIMETZ must wrap mod 24 hours
#define MaxTimeValue (usPerDay + usPerSecond - 1)

/**
 * Produce a TimeTz value from time (in microseconds) at
 * a timezone (in seconds, seconds field must be 0)
 */
static inline TimeTzADT setTimeTz(int64 time, int32 zone);
static inline TimeTzADT setTimeTz(int64 time, int32 zone) {
    Assert(time >= 0 && time <= MaxTimeValue);
    Assert(zone >= -MaxTimezone && zone <= MaxTimezone && zone % SECS_PER_MINUTE == 0);

// This should be changed, but databases will need to be updated
//    time = kMod(time + zone * usPerSecond, usPerDay); // in GMT seconds
//    zone = (secPerDay - zone) / SECS_PER_MINUTE;      // in minutes
    time += zone * usPerSecond;
    zone += secPerDay;

    return (time << ZoneFieldWidth) | zone;
}

/**
 * Get timezone from a TimeTz value (in seconds) (seconds field should be 0)
 */
static inline int32 getZoneTz(TimeTzADT time);
static inline int32 getZoneTz(TimeTzADT time) { // in seconds, +/- 24 hours
    int32 zone = time & ZoneMask;
// This should be changed, but databases will need to be updated
//    if (zone < secPerHour)              // if changed format
//        return secPerDay - zone * SECS_PER_MINUTE;
    return zone - secPerDay;
}

/**
 * Get the GMT time of the TimeTz value (in microseconds)
 */
static inline int64 getGMTTz(TimeTzADT time);
static inline int64 getGMTTz(TimeTzADT time) { // time in microseconds
    return kMod(time >> ZoneFieldWidth, usPerDay);
}

/**
 * Get the time at the timezone specified in TimeTz value itself
 * (in microseconds)
 */
static inline int64 getTimeTz(TimeTzADT time);
static inline int64 getTimeTz(TimeTzADT time) { // time in microseconds
    return kMod((time >> ZoneFieldWidth) - getZoneTz(time) * usPerSecond,
                usPerDay);
}

/**
 * Produce a Time value
 */
static inline TimeADT setTime(int64 time);
static inline TimeADT setTime(int64 time) {
    Assert(time >= 0 && time <= MaxTimeValue);
    return time;
}

/**
 * Get the time from a Time value
 */
static inline uint64 getTime(TimeADT time);
static inline uint64 getTime(TimeADT time) { // time in microseconds
    return time;
}

/**
 * Convert Timestamp to Unix time_t value (which is number of seconds since the Unix epoch)
 * The internal representation of Timestamps is number of microseconds since the 
 * Postgres epoch date. 
 * This will truncate the timestamp to number of seconds (instead of microseconds) 
 */
static inline time_t getUnixTimeFromTimestamp(Timestamp timestamp);
static inline time_t getUnixTimeFromTimestamp(Timestamp timestamp) {
    return (timestamp / usPerSecond) + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay; 
}


static inline time_t getUnixTimeFromTimestampTz(TimestampTz timestamp);
static inline time_t getUnixTimeFromTimestampTz(TimestampTz timestamp) {
    return (timestamp / usPerSecond) + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay; 
}

/**
 *Convert Unix time_t to Timestamp
 */
static inline Timestamp getTimestampFromUnixTime(time_t time);
static inline Timestamp getTimestampFromUnixTime(time_t time) {
    return ( (time - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay) * usPerSecond ); 
}

/**
 *Convert Unix time_t to TimestampTz
 */
static inline TimestampTz getTimestampTzFromUnixTime(time_t time);
static inline TimestampTz getTimestampTzFromUnixTime(time_t time) {
    return ( (time - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay) * usPerSecond ); 
}


/**
 * Convert Date to Unix Time
 */
static inline time_t getUnixTimeFromDate(DateADT date);
static inline time_t getUnixTimeFromDate(DateADT date) {
    return ( date + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * secPerDay; 
}

/**
 *Convert Unix time_t to Date
 */
static inline DateADT getDateFromUnixTime(time_t time);
static inline DateADT getDateFromUnixTime(time_t time) {
    return (time / secPerDay) - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE); 
}


/**
 * Convert Time to Unix Time
 */
static inline time_t getUnixTimeFromTime(TimeADT t);
static inline time_t getUnixTimeFromTime(TimeADT t) {
    return t / usPerSecond;
}

/**
 *Convert Unix time_t to Time
 */
static inline TimeADT getTimeFromUnixTime(time_t time);
static inline TimeADT getTimeFromUnixTime(time_t time) {
    return time * usPerSecond;
}

/// Format interval "<subtype>" where <subtype> often starts with a space
/// Call with char buf[64] to hold the result.
static inline void describeIntervalTypeMod(char *result, int32 typemod)
{
    int range = INTERVAL_RANGE(typemod);
    int precision = INTERVAL_PRECISION(typemod);
    const char *field1 = "", *field2 = "";

    if (range != INTERVAL_DAY2SECOND) {
        if (range & (range - 1)) { // if two or more bits are set
            if (range & INTERVAL_MASK(YEAR))
                field1 = " year to";
            else if (range & INTERVAL_MASK(DAY))
                field1 = " day to";
            else if (range & INTERVAL_MASK(HOUR))
                field1 = " hour to";
            else
                field1 = " minute to";
        }
        if (range & INTERVAL_MASK(SECOND))
            field2 = " second";
        else if (range & INTERVAL_MASK(MINUTE))
            field2 = " minute";
        else if (range & INTERVAL_MASK(HOUR))
            field2 = " hour";
        else if (range & INTERVAL_MASK(DAY))
            field2 = " day";
        else if (range & INTERVAL_MASK(MONTH))
            field2 = " month";
        else
            field2 = " year";
    }

    if ((range & INTERVAL_MASK(SECOND)) &&
        precision != MAX_SECONDS_PRECISION)
        snprintf(result, 64, "%s%s(%d)", field1, field2, precision);
    else
        snprintf(result, 64, "%s%s", field1, field2);
}

#endif
