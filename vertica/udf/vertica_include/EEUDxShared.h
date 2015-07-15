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
#ifndef EE_UDX_SHARED_H
#define EE_UDX_SHARED_H

#include "BasicsUDxShared.h"


namespace EE {

class VEval;                // fwd declaration for Vertica internal class
class UserDefinedProcess;   // fwd declaration for Vertica internal class
class UserDefinedTransform;
class UserDefinedAnalytic;
class UserDefinedAggregate;
class DataHolder;

namespace Loader {
class UserDefinedLoad; // fwd declaration for Vertica internal class
}

// Format of a variable length item
struct StringValue
{
    vsize slen;                         // string length; -1 for NULL
    uint32 sloc;                        // data_area-relative pointer to string
    char base[];                        // if sloc is NULL, string is here
    char *inlinePtr(){return base + sloc;}
};

/**
 * This class holds the strings used by a TupleBlock, VLHash, etc.
 * It is fixed length, applications either use a conservative max or go
 * until it is full.
 */
class DataArea
{
public:
    size_t sizeDA;                      // allocated size in bytes
    size_t freeDA;                      // current size in bytes

    void initializeDA(size_t sz)
    {
        sizeDA = sz;
        clearDA();
    }
    void clearDA() { freeDA = sizeof(DataArea); }
    char *entry(size_t e) {
        return reinterpret_cast<char *>(this) + e; }
    const char *centry(size_t e) const {
        return reinterpret_cast<const char *>(this) + e; }
};

// Inline functions for dealing with StringValue
// Three functions which return different types of pointer to the string area
inline
bool isInlineSV(const void *pv)
{
    const StringValue *p = static_cast<const StringValue *>(pv);
    return p->sloc == 0;
}
inline
const void *valueSV(const void *pv, const DataArea *da = NULL)
{
    const StringValue *p = static_cast<const StringValue *>(pv);
    if (isInlineSV(p))
        return p->base;
    Assert(da);
    return da->centry(p->sloc);
}
inline
const char *cvalueSV(const void *pv, const DataArea *da = NULL)
{
    return static_cast<const char *>(valueSV(pv, da));
}
inline
char *strSV(void *pv, DataArea *da = NULL)
{
    StringValue *p = static_cast<StringValue *>(pv);
    if (isInlineSV(p))
        return p->base;
    Assert(da);
    return da->entry(p->sloc);
}

inline
vsize lenSV(const void *pv)             // usually a char *
{
    const StringValue *p = static_cast<const StringValue *>(pv);
    return p->slen;
}
inline
void setLenSV(void *pv, vsize len)      // usually a char *
{
    StringValue *p = static_cast<StringValue *>(pv);
    p->slen = len;
}

inline
bool isNullSV(const void *pv)           // usually a char *
{
    return lenSV(pv) == StringNull;
}
inline
void setNullSV(void *pv)                // usually a char *
{
    StringValue *p = static_cast<StringValue *>(pv);
    p->slen = StringNull;
    p->sloc = 0;
}

// For the next three routines, while TupleBlocks still have pre-allocated
// space, da is ignored --
// as a placeholder, if you don't have the data_area address, use OutDA
#define OutDA NULL                      // placeholder

// allocate space for the StringValue, return length in bytes
// dap == OutDA or *dap == OutDA means string space is allocated at base[0]
inline
vsize allocSV(void *pv, DataArea **dap, vsize len)
{
    StringValue *p = static_cast<StringValue *>(pv);
    p->slen = len;
    if (len == StringNull) {
        p->sloc = 0;                    // no string needed
        return 0;
    }

    if (dap == OutDA || *dap == OutDA)
        p->sloc = 0;                    // use pre-allocated string area
    else {
        DataArea *da = *dap;
        p->sloc = da->freeDA;
        da->freeDA += len;
        if (da->freeDA > da->sizeDA) {
            ereport(INTERNAL,
                    (errmsg("DataArea overflow (%zu bytes needed, %zu allocated)",
                            da->freeDA, da->sizeDA),
                     errcode(ERRCODE_OUT_OF_MEMORY)));
        }
    }

    return len;
}

/// setSV sets the value of a StringValue from a pointer s and a length
/// a len of StringNull (-1) implies a Null and s is not used
/// space is allocated in the da; if pre-allocated call with dap or *dap=OutDA
inline
void setSV(void *pv, DataArea **dap, const void *s, vsize len)
{
    if (allocSV(pv, dap, len))
        vsmemcpy(strSV(pv, dap == OutDA ? NULL : *dap), s, len);
}

/// copySV copies one StringValue to another; Nulls are copied correctly
/// space is allocated in the pda; for an inline string call with pda=OutDA
inline
void copySV(void *pv, DataArea **pdap, const void *sv,
            const DataArea *sda = NULL /* = NULL is TEMP */)
{
    setSV(pv, pdap, valueSV(sv, sda), lenSV(sv));
}

/// eqSV returns -1->both Null, 0->not equal, 1->equal
/// so you can easily consider two Nulls to be equal to each other, or not
inline
int eqSV(const void *pa, const DataArea *daa,
         const void *pb, const DataArea *dab)
{
    vsize alen = lenSV(pa);
    if (alen != lenSV(pb))
        return 0;
    if (alen == StringNull)
        return -1;
    return vsmemne(valueSV(pa, daa), valueSV(pb, dab), alen) ? 0 : 1;
}

/// This is used for binary and varbinary. Don't use this function to do
/// character specific compare (e.g. locale).
inline
int eqSV(const void *pa, const void *pb)
{
    return eqSV(pa, NULL, pb, NULL);
}

/// Like memcmp, cmpSV returns -1->(a < b), 0->equal, 1->(a > b)
/// Null is greater than anything else; two Nulls are considered equal
inline
int cmpSV(const void *pa, const DataArea *daa,
          const void *pb, const DataArea *dab)
{
    vsize alen = lenSV(pa);
    vsize blen = lenSV(pb);
    if (alen == blen) {
        if (int(alen) <= 0) return 0;   // Null or empty strings
        return memcmp(valueSV(pa, daa), valueSV(pb, dab), alen);
    }
    if (alen < blen) {
        if (blen == StringNull) return -1; // a is less than Null
        int r = memcmp(valueSV(pa, daa), valueSV(pb, dab), alen);
        return (r == 0 ? -1 : r);
    }
    {
        if (alen == StringNull) return  1; // Null is greater than b
        int r = memcmp(valueSV(pa, daa), valueSV(pb, dab), blen);
        return (r == 0 ?  1 : r);
    }
}

inline
int cmpSV(const void *pa, const void *pb)
{
    return cmpSV(pa, NULL, pb, NULL);
}

}

#endif
