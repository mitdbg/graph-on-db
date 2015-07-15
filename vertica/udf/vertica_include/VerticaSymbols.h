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
 
 /**
 * \internal
 * \file   VerticaSymbols.h
 * \brief  Defines error-reporting macros. 
 */
 
#ifndef VERTICA_SYMBOLS_H
#define VERTICA_SYMBOLS_H

/*
 * Include symbols that have different definition for UDx and Vertica server code
 */

/** \def vt_report_error(errcode, args...)
 * Generates a runtime error from user code which is reported by the
 * server. Internally this throws a C++ exception with the same stack unwinding
 * semantics as normal C++ exceptions.
 *
 * @param errcode: produce the with the specified errcode.
 * @param args: error message with printf (varargs style) format specifiers.
 */
#define vt_report_error(errcode, args...) \
    do { \
        std::stringstream err_msg; \
        Vertica::makeErrMsg(err_msg, args); \
        Vertica::vt_throw_exception(errcode, err_msg.str(), __FILE__, __LINE__); \
    } while(0)

#define ereport(elevel, rest) \
    do { \
        std::stringstream err_msg; \
        rest; \
        std::string msg = "User code caused Vertica to throw exception \""; \
        msg += err_msg.str() + "\"";                                    \
        Vertica::vt_throw_exception(0, msg, __FILE__, __LINE__); \
    } while(0)

#define errcode(arg) dummy()
#define errdetail(args...) Vertica::makeErrMsg(err_msg, args)
#define errhint(args...) Vertica::makeErrMsg(err_msg, args)
#define errmsg(args...) Vertica::makeErrMsg(err_msg, args)

#define VIAssert(expr) \
    do { \
        if (!(expr)) { ereport(INTERNAL, (errmsg("VIAssert(%s) failed", __STRING(expr)))); } \
    } while(0)
#define VAssert(expr) \
    do { \
        if (!(expr)) { ereport(PANIC, (errmsg("VAssert(%s) failed", __STRING(expr)))); } \
    } while(0)
#define Assert(expr) VAssert(expr)

#endif
