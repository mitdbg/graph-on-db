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
/***
 * Brief: Contains code for integer (or fixed point) code.
 *
 * Description: This is for performing extended precision integer arithmetic.
 *   The chosen internal representation is pure binary (64-bit full words)
 *   Other nuances are inherited from Vertica integers (such as the NULL rep)
 *   The format is 2's complement to make best use of the system math
 *
 * Notes:  This is a purely little-endian scheme; big-endian could be defined
 *  as well.
 *
 * TODOs: This can still be optimized significantly, by making better use of
 *   64-bit instructions and so on.  It is just a first cut at the API.
 *
 * Creation Date: 1-Jul-2008
 */
#ifndef BASICS_BIGINT_H
#define BASICS_BIGINT_H

#include <algorithm>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#define BI_DEBUG 0
#if BI_DEBUG

inline static void dprintHex(const uint64 *p, int nw)
{
    int i;
    for (i=0; i<nw; ++i) {
        if (p[i]) break;
    }
    if (i == nw){printf("0"); return;}
    printf("%llx", p[i++]);
    for (; i<nw; ++i) printf("%016llx", p[i]);
}

#endif

#include "BasicsUDxShared.h"

#endif
