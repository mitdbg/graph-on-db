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
 * Create Date: Feb 10, 2011
 */
#include "Vertica.h"
#include "BuildInfo.h"    

namespace Vertica {

extern "C" {
    VT_THROW_EXCEPTION vt_throw_exception = 0; // function pointer to throw exception
    ServerFunctions *VERTICA_INTERNAL_fns = NULL;
}

// setup global function pointers, called once the UDx library is loaded
extern "C" void setup_global_function_pointers(VT_THROW_EXCEPTION throw_exception, ServerFunctions *fns)
{
    vt_throw_exception = throw_exception;
    VERTICA_INTERNAL_fns = fns;
}

extern "C" void get_vertica_build_info(VerticaBuildInfo &vbi)
{
    vbi.brand_name    = VERTICA_BUILD_ID_Brand_Name;
    vbi.brand_version = VERTICA_BUILD_ID_Brand_Version;
    vbi.codename      = VERTICA_BUILD_ID_Codename;
    vbi.build_date    = VERTICA_BUILD_ID_Date;
    vbi.build_machine = VERTICA_BUILD_ID_Machine;
    vbi.branch        = VERTICA_BUILD_ID_Branch;
    vbi.revision      = VERTICA_BUILD_ID_Revision;
    vbi.checksum      = VERTICA_BUILD_ID_Checksum;
}

// This big enough for anything reasonable
#define MSG_BUF 2048

int makeErrMsg(std::stringstream &err_msg, const char *fmt, ...)
{
    va_list va;
    char msg[MSG_BUF];

    va_start(va, fmt);
    vsnprintf(msg, MSG_BUF, gettext(fmt), va);
    va_end(va);

    err_msg << msg;
    return 0;
}

void dummy() {}


// Global Library Manifest object & getter function
UserLibraryManifest &GlobalLibraryManifest()
{
    static UserLibraryManifest singleton;
    return singleton;
}

extern "C" UserLibraryManifest* get_library_manifest()
{
    return &GlobalLibraryManifest();
}
namespace Basics {
#include "NumericTables.h"
}

#include "NumericSupport.cpp"

// Date/Time-parsing functions
DateADT dateIn(const char *str, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateIn(str, report_error);
}

TimeADT timeIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeIn(str, typmod, report_error);
}

TimeTzADT timetzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzIn(str, typmod, report_error);
}

Timestamp timestampIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampIn(str, typmod, report_error);
}

TimestampTz timestamptzIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzIn(str, typmod, report_error);
}

Interval intervalIn(const char *str, int32 typmod, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->intervalIn(str, typmod, report_error);
}

DateADT dateInFormatted(const char *str, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->dateInFormatted(str, format, report_error);
}

TimeADT timeInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timeInFormatted(str, typmod, format, report_error);
}

TimeTzADT timetzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timetzInFormatted(str, typmod, format, report_error);
}

Timestamp timestampInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestampInFormatted(str, typmod, format, report_error);
}

TimestampTz timestamptzInFormatted(const char *str, int32 typmod, const std::string format, bool report_error) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    return VERTICA_INTERNAL_fns->timestamptzInFormatted(str, typmod, format, report_error);
}

Oid getUDTypeOid(const char *typeName) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeOid(typeName);
}

Oid getUDTypeUnderlyingOid(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->getUDTypeUnderlyingOid(typeOid);
}

bool isUDType(Oid typeOid) {
    VIAssert(VERTICA_INTERNAL_fns != NULL);
    return VERTICA_INTERNAL_fns->isUDType(typeOid);
}

void log(const char *format, ...)
{
    VIAssert(VERTICA_INTERNAL_fns != NULL);  // Must be set by Vertica
    va_list ap;
    va_start(ap, format);
    VERTICA_INTERNAL_fns->log(format, ap);
    va_end(ap);
}

} // namespace Vertica
