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
 *
 * Description: Support code for UDx subsystem
 *
 * Create Date: Mar 13, 2012
 */
extern DateADT dateIn(const char *str, bool report_error);
extern TimeADT timeIn(const char *str, int32 typmod, bool report_error);
extern TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error);
extern Timestamp timestampIn(const char *str, int32 typmod, bool report_error);
extern TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error);
extern Interval intervalIn(const char *str, int32 typmod, bool report_error);

extern DateADT dateInFormatted(const char *str, const std::string format, bool report_error);
extern TimeADT timeInFormatted(const char *str, int32 typmod, const std::string format, bool report_error);
extern TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error);
extern Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string format, bool report_error);
extern TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error);
