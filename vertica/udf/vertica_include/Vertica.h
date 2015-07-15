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
#ifndef VERTICA_H
#define VERTICA_H

/*
 * Include definitions only used for UDx, but not Vertica server
 */

/** @file
 * \brief Contains the classes needed to write User-Defined things in Vertica
 */

#include <vector>
#include <list>
#include <string>
#include <map>
#include <algorithm>
#include <cassert>
#include <sstream>
#include <math.h>    // for powl()
#include <sstream>

#include <stdarg.h>  // for va_list and friends
#include <stdio.h>   // for vsnprintf()
#include <locale.h>  // for gettext()
#include <libintl.h> // for gettext()
#include <string.h>  // for memcmp()
#include <stdlib.h>  // for abs()
#include <stdint.h>  // for [u]int{8|16|32|64}_t

#if !defined(__Linux32__) && !defined(__Linux64__)
#define __Linux64__
#endif

/** Defines classes for Vertica User Defined Functions 
*/
namespace Vertica {

#include "VerticaSymbols.h"
#include "VerticaUDx.h"
#include "VerticaUDl.h"
#include "ServerFunctions.h"
#include "Logging.h"
#include "DateTimeParsing.h"

/* @cond INTERNAL Returns the global library manifest object */
UserLibraryManifest &GlobalLibraryManifest();
/* @endcond */

struct UDxRegistrar
{
    UDxRegistrar(const char *name) { GlobalLibraryManifest().registerObject(name); }
};

} // namespace Vertica

/// \def RegisterFactory(clazz)
/// @param class The name of the class to register as a UDx factory.
/// Helper macro for registering UDx's with Vertica
///
/// To use: Extend a subclass of UDXFactory (e.g. ScalarFunctionFactory) and then
/// call RegisterFactory with that class name.
///
/// For example:
///
/// ...
/// class MyFactory : public ScalarFunctionFactory
/// ...
///
/// RegisterFactory(MyFactory);
/// 
#define RegisterFactory(clazz) \
    clazz clazz##_instance; \
    extern "C" Vertica::UDXFactory *get##clazz() { return &clazz##_instance; } \
    Vertica::UDxRegistrar clazz##_registrar(#clazz)

/**
 * InlineAggregate() is used to implement the virtual function "aggregateArrs()"
 * for AggregateFunctions. This allows aggregate functions to work on blocks at a
 * time. This macro should be called from inside an AggregateFunction - for a
 * reference, check the examples in the example folder.
 */
#define InlineAggregate() \
    virtual void aggregateArrs(ServerInterface &srvInterface, void **dstTuples,\
                               int doff, const void *arr, int stride, const void *rcounts,\
                               int rcstride, int count, IntermediateAggs &intAggs,\
                               std::vector<int> &intOffsets, BlockReader &arg_reader) {\
        char *arg = const_cast<char*>(static_cast<const char*>(arr));\
        const uint8 *rowCountPtr = static_cast<const uint8*>(rcounts);\
        for (int i=0; i<count; ++i) {\
            vpos rowCount = *reinterpret_cast<const vpos*>(rowCountPtr);\
            char *aggPtr = static_cast<char *>(dstTuples[i]) + doff;\
            updateCols(arg_reader, arg, rowCount, intAggs, aggPtr, intOffsets);\
            aggregate(srvInterface, arg_reader, intAggs);\
            arg += rowCount * stride;\
            rowCountPtr += rcstride;\
        }\
    }\

#endif
