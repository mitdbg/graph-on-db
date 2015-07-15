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
#ifndef SERVERFUNCTIONS_H_
#define SERVERFUNCTIONS_H_

// INTERNAL class
// NOT FOR instantiation or overloading/subclassing in external code
/// @cond INTERNAL
class ServerFunctions {
public:

    // *************
    // !! WARNING !!
    // *************
    //
    // The following are INTERNAL FUNCTIONS.
    // They should NEVER be called directly except in Vertica-provided code!
    // THE FOLLOWING APIs WILL CHANGE between Vertica versions!
    // Their functionality is exposed directly in other (public) methods in
    // Vertica.h; use those function calls in the public API instead, as they
    // are supported and will receive updates and optimizations in the future.

    virtual DateADT dateIn(const char *str, bool report_error) = 0;

    virtual TimeADT timeIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual Timestamp timestampIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual Interval intervalIn(const char *str, int32 typmod, bool report_error) = 0;

    virtual DateADT dateInFormatted(const char *str, const std::string &format, bool report_error) = 0;

    virtual TimeADT timeInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string &format, bool report_error) = 0;

    virtual void log(const char* format, va_list ap) = 0;

    virtual Oid getUDTypeOid(const char *typeName) = 0;

    virtual Oid getUDTypeUnderlyingOid(Oid typeOid) = 0;

    virtual bool isUDType(Oid typeOid) = 0;

    // Virtual destructor; may be overridden
    virtual ~ServerFunctions() {}
};
/// @endcond

#endif // SERVERFUNCTIONS_H_
