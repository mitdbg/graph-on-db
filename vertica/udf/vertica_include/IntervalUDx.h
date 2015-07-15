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
 * Description: Provides helpful functions for Interval and IntervalYM types.
 *
 * Create Date: Mar 15, 2012
 */
#ifndef VINTERVAL_UDX_H
#define VINTERVAL_UDX_H

/**
* \internal
* \file IntervalUDx.h
* \brief Utilities for handling Intervals
*/

/* @cond INTERNAL */
/// Compute n divided_by m with positive remainder, even if n is negative
static inline int64 _Mod(int64 _n, int64 _m) {
    return _n < 0 ? ~(~(_n) % (_m)) + (_m) : (_n) % (_m);
}
    
/// Compute n divided_by m with positive remainder, even if n is negative
static inline int64 _Div(int64 _n, int64 _m) {
    return _n < 0 ? ~(~(_n) / (_m)) : (_n) / (_m);
}
/* @endcond */

/** 
 * @brief Representation of an Interval in Vertica. 
 */
class VInterval {
public:

    /** 
     * @brief Break up an Interval and set the arguments.
     * @return None
     * @param[i] Vertica Interval.
     * @param[days] Number of days in the interval.
     * @param[hour] Number of hours in the interval.
     * @param[min]  Number of minutes in the interval.
     * @param[sec]  Number of seconds including fractions of a second.
     */
    
    static inline void breakUp(Interval i, int64& days, 
            int64& hour, int64& min, float& sec) {
        days = _Div(i, usPerDay);
        
        int64 time = _Mod(i, usPerDay);

        hour = time / usPerHour;
        time -= hour * usPerHour;
        min = time / usPerMinute;
        time -= min * usPerMinute;
        sec = (float) time / usPerSecond;

        return;
    }
};

/** 
 * @brief Representation of an IntervalYM in Vertica. An Interval can be 
 *        broken up into years and months
 */
class VIntervalYM {
public:
    
    /** 
     * @brief Break up an Interval and set the arguments.
     * @return None
     * @param[i] Vertica IntervalYM.
     * @param[years]  Number of years in the interval.
     * @param[months] Number of months in the interval.
     */

    static inline void breakUp(IntervalYM i, int64& years,
            int64& months) {
        years = _Div(i, MONTHS_PER_YEAR);
        months = _Mod(i, MONTHS_PER_YEAR);
    }

};
#endif
