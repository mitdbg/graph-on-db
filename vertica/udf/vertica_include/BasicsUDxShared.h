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
 * Create Date: Feb 10, 2011 
 */

/**
* \internal
* \file   BasicsUDxShared.h
* \brief  Support code for UDx subsystem. 
*/


#ifndef BASICS_UDX_SHARED_H
#define BASICS_UDX_SHARED_H

#include "Oids.h"
#include "PGUDxShared.h"
#include <math.h>

/**
* \typedef signed char           int8;
* \brief 8-bit integer
*/
typedef signed char           int8;
/**
* \typedef unsigned char         uint8;
* \brief unsigned 8-bit integer
*/
typedef unsigned char         uint8;
/**
* \typedef signed short          int16;
* \brief signed 16-bit integer
*/
typedef signed short          int16;
/**
* \brief unsigned 16-bit integer
*/
typedef unsigned short        uint16;
/**
* \brief signed 32-bit integer
*/
typedef signed int            int32;
/**
* \brief unsigned 32-bit integer
*/
typedef unsigned int          uint32;
/**
* \brief signed 64-bit integer
*/
typedef signed long long      int64;
/**
* \brief unsigned 64-bit integer
*/
typedef unsigned long long    uint64;

/**
 * \brief unsigned 8-bit integer
 */
typedef uint8                 byte;    
/**
 * \brief  64-bit vertica position
*/
typedef unsigned long long    vpos;    
/**
 * \brief  vertica 8-bit boolean (t,f,null)
*/
typedef uint8                 vbool;   
/**
 * \brief vertica 64-bit integer (not int64)
*/
typedef signed long long      vint;    
/**
 * \brief vertica 32-bit block size
*/ 
typedef uint32                vsize;    

/**
 * \brief Represents Vertica 64-bit floating-point type
 * for NULL checking use vfloatIsNull(), assignment to NULL use vfloat_null
 * for NaN checking use vfloatIsNaN(), assignment to NaN use vfloat_NaN
 */
typedef double                vfloat;   // vertica 64-bit float
/**
 * \brief vertica 80-bit intermediate result
*/
typedef long double           ifloat;   // vertica 80-bit intermediate result

/**
* \brief Vertica timestamp data type. 
*/
typedef int64 TimestampTz;


// SQL defines VUnknown to be the same value as vbool_null
// "I kept seeing ones and zeroes it was horrible. . .
//     I think I saw a two *shivers*"
//                            -- Bender
/// Enumeration for Boolean data values
enum VBool {VFalse = 0, VTrue = 1, VUnknown = 2}; // Be careful changing!

/// Value for Integer NULLs.
const vint vint_null  =   0x8000000000000000LL;
/// Value for Boolean NULL
const vbool vbool_null =  VUnknown;
/// Value for Boolean TRUE
const vbool vbool_true =  VTrue;
/// Value for Boolean FALSE
const vbool vbool_false =  VFalse;

/**
* \brief Indicates NULL strings
*/
#define StringNull vsize(-1)            // Used to indicate NULL strings
// isNullSV is defined in EEUDxShared.h
#if 1 // being changed
/**
* \brief Indicates NULL char
*/
const char char_null  =   0xff;
/**
* \brief Indicates NULL byte
*/
const byte byte_null  =   0xff;
#endif


/**
*  \INTERNAL Version of memcpy optimized for vertica's small data types.
*  Inline function for copying small things of arbitrary length.
*/
inline void vsmemcpy(void *dst, const void *src, size_t len){
    uint8 *d = static_cast<uint8 *>(dst);
    const uint8 *s = static_cast<const uint8 *>(src);

    while (len >= 8){
       *reinterpret_cast<vint *>(d) = *reinterpret_cast<const vint *>(s);
       d+=8; s+=8; len-=8;
    }
    if (len >= 4){ // eliminate on machines with loop detectors?
       *reinterpret_cast<uint32 *>(d) = *reinterpret_cast<const uint32 *>(s);
       d+=4; s+=4; len-=4;
    }
    while (len){
       *(d) = *(s);
       ++d; ++s; --len;
    }
}

/**
 * \Brief Vertica memcpy 
*/
#define VMEMCPY(x,y,z) do {                                         \
    if ((z) == 8)                                                   \
        *reinterpret_cast<vint *>(x) = *reinterpret_cast<const vint *>(y); \
    else vsmemcpy(x, y, z);                                         \
    } while (0)
/**
 * \brief Version of memcmp for detecting not equal only.
 *   Inline function for comparing small things of arbitrary length. 
 */
inline bool vsmemne(const void *p1, const void *p2, size_t len){
    const uint8 *v1 = static_cast<const uint8 *>(p1);
    const uint8 *v2 = static_cast<const uint8 *>(p2);

    if (len >= 8) {
        if (*reinterpret_cast<const vint *>(v1) !=
            *reinterpret_cast<const vint *>(v2))
            return true;
        v1+=8; v2+=8; len-=8;
        if (len >= 8) {
            if (*reinterpret_cast<const vint *>(v1) !=
                *reinterpret_cast<const vint *>(v2))
                return true;
            v1+=8; v2+=8; len-=8;
        }
    }

    bool diff = false;

    while (len >= 8){
       diff |= *reinterpret_cast<const vint *>(v1) 
                 != *reinterpret_cast<const vint *>(v2);
       v1+=8; v2+=8; len-=8;
    }
    if (len >= 4){ // eliminate on machines with loop detectors?
       diff |= *reinterpret_cast<const uint32 *>(v1) 
                 != *reinterpret_cast<const uint32 *>(v2);
       v1+=4; v2+=4; len-=4;
    }
    while (len){
       diff |= *reinterpret_cast<const uint8 *>(v1) 
                 != *reinterpret_cast<const uint8 *>(v2);
       ++v1; ++v2; --len;
    }

    return diff;
}

/*
 * \brief  Version of memset for setting to 0 optimized for vertica's small data types.
 *  Inline function for zeroing small things of arbitrary length.
 */
inline void vsmemclr(void *dst, size_t len){
    uint8 *d = static_cast<uint8 *>(dst);
    while (len >= 8){
        *reinterpret_cast<uint64 *>(d) = 0;
        d+=8; len-=8;
    }
    if (len >= 4){
       *reinterpret_cast<uint32 *>(d) = 0;
       d+=4; len-=4;
    }
    while (len){
       *(d) = 0;
       ++d; --len;
    }
}

/*
 * \brief non-signalling NAN
 *
 */
const union FIunion {
    vint vi;
    vfloat vf;
} vfn = {0x7ffffffffffffffeLL}; // our non-signalling NAN (fe helps valgrind)
/*
 * \brief Indicates NULL floats
 */
const vfloat vfloat_null = vfn.vf;
/*
 * \brief Checks if a float is NULL
 *
 */
inline bool vfloatIsNull(const vfloat* vf) {
    return !vsmemne(vf, &vfloat_null, sizeof(vfloat)); }
/*
 * \brief  Checks if a float is NULL
 *
 */
inline bool vfloatIsNull(const vfloat vf) { return vfloatIsNull(&vf); }

/*
 * \brief  The x86 generated version of NaN is not the c99 version (math.h NAN)
 */
const union FIunion vfNaN = {0xFFF8000000000000LL};
/*
 * \brief  Indicates float NAN
q */
#define vfloat_NaN vfNaN.vf
/*
 * \brief  Check if a float is NAN
 */
inline bool vfloatIsNaN(const vfloat *f) {
   vint v = *reinterpret_cast<const vint *>(f);
   return (((v & 0x7FF0000000000000LL) == 0x7FF0000000000000LL) 
           && ((v & 0x000FFFFFFFFFFFFFLL) != 0)); // big exp but not Infinity
}
/*
 * \brief Check if a float is NAN
 *
 */
inline bool vfloatIsNaN(const vfloat f) { return vfloatIsNaN(&f); }

/// basic utilities mainly for data types.
namespace Basics
{
// fwd declaration
bool Divide32(uint64 hi, uint64 lo, uint64 d, uint64 &_q, uint64 &_r);

// length is the Vertica data length in bytes; for CHARs it is typmod-4
// For VARCHARs it is the current or desired data length if known, otherwise -1.

// typmod is the 'Type Modifier' -- used for precision in time/timestamp, etc.
// For CHAR/VARCHAR fields PG adds 4 to the user-specified field width to
// allow for the four-byte string length field it prepends to the string; if
// the maximum length is unspecified, typmod is set to -1.

/* From pg_attribute.h: "atttypmod records type-specific data supplied at table
 * creation time (for example, the max length of a varchar field).  It is
 * passed to type-specific input and output functions as the third
 * argument.  The value will generally be -1 for types that do not need
 * typmod." */

// Special code for interpreting numeric typemod
#define NUMERIC_DSCALE_MASK 0x1FFF      /* allow an extra sign/type bit */

/// Get Numeric precision from typmod
inline int32 getNumericPrecision(int32 typmod)
{
    return ((typmod - VARHDRSZ) >> 16) & 0xffff; // unsigned
}
/// Get Numeric scale from typmod
inline int32 getNumericScale(int32 typmod)
{
    return (typmod - VARHDRSZ) & NUMERIC_DSCALE_MASK; // unsigned
}
/// Get Numeric word count from precision
inline int32 getNumericWordCount(int32 precision)
{
    // TODO: Replace with a formula that is not as conservative?
    //   (a word holds a little over 19 digits,
    //    except first holds slightly less 19 due to sign bit)
    return precision / 19 + 1;
}
/// Get Numeric data length from typmod
static int32 getNumericLength(int32 typmod)
{
    // Eliminate all -1 usage
    VIAssert(typmod != -1);
    int32 precision = getNumericPrecision(typmod);
    assert (precision > 0);
    //assert (precision >= 0);
    assert (getNumericScale(typmod) >= 0);
    return 8 * getNumericWordCount(precision);
}

/// Return true if these have the same EE representation
inline bool isSimilarNumericTypmod(int32 a, int32 b)
{
    return (Basics::getNumericScale(a) == Basics::getNumericScale(b))
       && (Basics::getNumericLength(a) == Basics::getNumericLength(b));
}

/// Used to scale by factors of 5.
struct FiveToScale
{
    unsigned scale;
    unsigned wordCount;
    const uint64 *value;
};

extern const FiveToScale fiveToScales[];

// Multiply u * v; results in p1, p2; 
#if defined(__Linux64__)
#define Multiply(u, v, p1, p2)                          \
    asm ("\tmul %%rdx\n"                                \
         : "=d"(p1), "=a"(p2) /* 128-bit product */     \
         : "d"(u), "a"(v)     /* Input operands */      \
         : "cc"               /* flags clobbered */ )
#else
#define Multiply(u, v, p1, p2)                           \
do {                                                       \
    uint64 _u = u, _v = v;                                 \
    uint64 lw = (_u & 0xFFFFFFFFLLU) * (_v & 0xFFFFFFFFLLU); \
    uint64 mw1 = (_u & 0xFFFFFFFFLLU) * (_v >> 32);          \
    uint64 mw2 = (_u >> 32) * (_v & 0xFFFFFFFFLLU);          \
    uint64 hw = (_u >> 32) * (_v >> 32);                     \
    p1 = hw + (mw1 >> 32) + (mw2 >> 32);                   \
    uint64 rla = lw + (mw1 << 32);                         \
    if (rla < lw) ++p1;                                    \
    p2 = rla + (mw2 << 32);                                \
    if (p2 < rla) ++p1;                                    \
} while (0)
#endif

// Divide u1u2 / v1, where u1 < v1; results in q and r
#if defined(__Linux64__)
#define Divide(u1,u2, v1, q, r)                         \
    asm ("\tdiv %%rcx\n"                                \
         : "=a"(q), "=d"(r) /* Out operands */          \
         : "d"(u1), "a"(u2), "c"(v1) /* In operands */  \
         : "cc" /* flags clobbered */ )
#else
// replace this with x86 asm using real div, etc., if anyone cares
#define Divide(u1,u2, v1, q, r)                         \
    Basics::Divide32(u1,u2, v1, q, r)
#endif

#define Min(x, y)               ((x) < (y) ? (x) : (y))

// This is just for a namespace kind of thing
/** Holds integer utilities
*/
struct BigInt
{
// Constants for doing scaling by factors of 10
static const int powerOf10Sizes[1200];
static const uint64 * const powerOf10Words[1200];
/** 
* \brief  cpoy nWords from src to buf 
*/
static void copy(void *buf, const void *src, int nWords)
{
    const uint64 *ubuf = static_cast<const uint64 *>(src);
    uint64 *ubufo = static_cast<uint64 *>(buf);

    for (int i=0; i<nWords; ++i) ubufo[i] = ubuf[i];
}

/** \fn setZero(void *buf, int nWords)
* \brief Set an integer to 0 
*/
static void setZero(void *buf, int nWords){
    int64 * ibuf = static_cast<int64 *>(buf);
    for (int i=0; i<nWords; ++i) ibuf[i] = 0;
}

/** 
 * \brief Check an integer for 0
* 
 * \note Optimized for small nWords; scan from end and break out of loop
 */
static bool isZero(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   int64 res = 0;
   for (int i=0; i<nWords; ++i) {
       res |= ibuf[i];
   }
   return res == 0;
}

/** 
* \brief Set an integer to NULL 
*/
static void setNull(void *buf, int nWords){
    int64 *ibuf = static_cast<int64 *>(buf);
    Assert (nWords > 0);
    ibuf[0] = vint_null;
    for (int i=1; i<nWords; ++i) ibuf[i] = 0;
}

/** 
 * \brief Check an integer for NULL
 */
static bool isNull(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   if (ibuf[0] != vint_null) return false;
   int64 res = 0;
   for (int i=1; i<nWords; ++i) {
       res |= ibuf[i];
   }
   return res == 0;
}

/** 
 * \brief Check if integer is less than zero
 */
static bool isNeg(const void *buf, int nWords){
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   return ibuf[0] < 0;
}

/** 
 * \brief Check integers for equality
 * 
 * \note Optimized for small nWords; scan from end and break out of loop
 */
static bool isEqualNN(const void *buf1, const void *buf2, int nWords){
   const int64 *ibuf1 = static_cast<const int64 *>(buf1);
   const int64 *ibuf2 = static_cast<const int64 *>(buf2);
   int64 res = 0;
   for (int i=0; i<nWords; ++i) {
       res |= ibuf1[i] - ibuf2[i];
   }
   return res == 0;
}

/** 
 * \brief Compare integers, return -1, 0, 1
 */
static int compareNN(const void *buf1, const void *buf2, int nWords){
   Assert (nWords > 0);
   const int64 *ibuf1 = static_cast<const int64 *>(buf1);
   const int64 *ibuf2 = static_cast<const int64 *>(buf2);
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);

   if (ibuf1[0] != ibuf2[0]) return ibuf1[0] < ibuf2[0] ? -1 : 1;

   for (int i=1; i<nWords; ++i) {
       if (ubuf1[i] != ubuf2[i]) return ubuf1[i] < ubuf2[i] ? -1 : 1;
   }
   return 0; // ==
}

/** 
 * Compare integers, return -1, 0, 1
 */
static int ucompareNN(const void *buf1, const void *buf2, int nWords){
   Assert (nWords > 0);
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);

   for (int i=0; i<nWords; ++i) {
       if (ubuf1[i] != ubuf2[i]) return ubuf1[i] < ubuf2[i] ? -1 : 1;
   }
   return 0; // ==
}

/** 
 * \brief Increment by 1
 */
static void incrementNN(void *buf, int nWords) {
    uint64 *ubuf = static_cast<uint64 *>(buf);
    uint64 cf = 1;
    int i = nWords;
    while (cf && i>0) {
        --i;
        ubuf[i] += cf;
        cf = (ubuf[i] == 0);
    }
}

/** 
 * \brief Invert the sign of a number, performed in place, using the invert and +1
 *   method, turns out NULLs are OK in this sense
 */
static void invertSign(void *buf, int nWords) {
   uint64 *ubuf = static_cast<uint64 *>(buf);
   for (int i=0; i<nWords; ++i) ubuf[i] ^= 0xFFFFFFFFFFFFFFFFLLU;
   incrementNN(ubuf, nWords);
}

/** 
 * \brief Add together 2 numbers w/ same number of digits
 *  No null handling
 * Returns carry
 */
static uint64 addNN(void *outBuf, const void *buf1, const void *buf2, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]+ubuf2[i];
       if (ubuf1[i] > rw) ++ncf;
       uint64 rw2 = rw+cf;
       if (rw > rw2) ++ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
   return cf;
}

/** 
 * \brief Add number on to a temporary
 *  No null handling
 * Returns carry
 */
static uint64 accumulateNN(void *outBuf, const void *buf1, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]+ubufo[i];
       if (ubufo[i] > rw) ++ncf;
       uint64 rw2 = rw+cf;
       if (rw > rw2) ++ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
   return cf;
}


/** 
 * \brief Subtract 2 numbers w/ same number of digits
 *  No null handling
 */
static void subNN(void *outBuf, const void *buf1, const void *buf2, int nWords) {
   const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
   const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
   uint64 *ubufo = static_cast<uint64 *>(outBuf);

   uint64 cf = 0;
   for (int i=nWords-1; i>=0; --i)
   {
       uint64 ncf = 0;
       uint64 rw = ubuf1[i]-ubuf2[i];
       if (ubuf1[i] < rw) --ncf;
       uint64 rw2 = rw+cf;
       if (rw < rw2) --ncf;
       ubufo[i] = rw2;
       cf = ncf;
   }
}

/** 
 * 
 * \brief Shift a BigInt to the right (>>) by the given number of bits.
 * The given BigInt must be positive and non-null.
 */
static void shiftRightNN(void *buf, unsigned nw, unsigned bitsToShift)
{
    Assert(nw > bitsToShift/64);
    uint64 *bbuf = static_cast<uint64 *>(buf);
    unsigned from = nw - (bitsToShift / 64) - 1, to = nw - 1;
    unsigned shift = bitsToShift % 64;
    if (shift==0) {
        for ( ; from > 0; --from) {
            bbuf[to--] = bbuf[from];
        }
        bbuf[to] = bbuf[from];
    } else {
        for ( ; from > 0; --from) {
            bbuf[to--] = (bbuf[from-1] << (64 - shift)) | (bbuf[from] >> shift);
        }
        bbuf[to] = bbuf[from] >> shift;
    }
    while (to > 0) bbuf[--to] = 0;
}

/**
 * 
 * \brief Shift a BigInt to the left (<<) by the given number of bits.
 * The given BigInt must be positive and non-null.
 */
static void shiftLeftNN(void *buf, unsigned nw, unsigned bitsToShift)
{
    Assert(nw > bitsToShift/64);
    uint64 *bbuf = static_cast<uint64 *>(buf);
    unsigned from = bitsToShift / 64, to = 0;
    unsigned shift = bitsToShift % 64;
    if (shift==0) {
        for ( ; from < nw-1; ++from) {
            bbuf[to++] = bbuf[from];
        }
        bbuf[to] = bbuf[from];
    } else {
        for ( ; from < nw-1; ++from) {
            bbuf[to++] = (bbuf[from] << shift) | (bbuf[from+1] >> (64 - shift));
        }
        bbuf[to] = bbuf[from] << shift;
    }
    while (to < nw-1) bbuf[++to] = 0;
}

/**
 * \brief Load from 64-bit signed int (does not handle NULL inside)
 */
static void fromIntNN(void *buf, int nWords, int64 val)
{
   uint64 *ubufo = static_cast<uint64 *>(buf);
   Assert (nWords > 0);

   ubufo[nWords - 1] = uint64(val);
   // Sign extend
   uint64 ext = (val & vint_null) ? -1LLU : 0;
   for (int i=0; i<nWords - 1; ++i) ubufo[i] = ext;
}

/**
 * \brief Convert to int (return false if there was an overflow)
 */
static bool toIntNN(int64 &out, const void *buf, int nWords)
{
   const int64 *ibuf = static_cast<const int64 *>(buf);
   Assert (nWords > 0);
   out = ibuf[nWords-1];
   if (out & vint_null) {
       for (int i=0; i<nWords - 1; ++i) {
           if (ibuf[i] != -1LL) return false;
       }
   }
   else {
       for (int i=0; i<nWords - 1; ++i) {
           if (ibuf[i] != 0) return false;
       }
   }
   return true;
}

/**
 * \brief Calculate number of words that are actually used
 *  to represent the value (amount left after stripping leading 0's)
 */
static int usedWordsUnsigned(const void *buf, int nWords)
{
    const uint64 *ubuf = static_cast<const uint64 *>(buf);
    int unusedWords = 0;
    for (; (unusedWords < nWords) && !ubuf[unusedWords]; ++unusedWords) ;
    return nWords - unusedWords;
}

static void setNumericBoundsFromType(uint64 *numUpperBound,
                                     uint64 *numLowerBound, 
                                     int nwdso, int32 typmod)
{
    int prec = PRECISIONFROMTYPMOD(typmod);
    int length = nwdso << 3;
    vsmemclr(numUpperBound, length);
    vsmemcpy(numUpperBound + nwdso - Basics::BigInt::powerOf10Sizes[prec],
             Basics::BigInt::powerOf10Words[prec],
             Basics::BigInt::powerOf10Sizes[prec] * 8);
    Basics::BigInt::invertSign(numUpperBound, nwdso);
    Basics::BigInt::incrementNN(numUpperBound, nwdso);
    Basics::BigInt::invertSign(numUpperBound, nwdso);
    vsmemclr(numLowerBound, length);
    vsmemcpy(numLowerBound + nwdso - Basics::BigInt::powerOf10Sizes[prec],
             Basics::BigInt::powerOf10Words[prec],
             Basics::BigInt::powerOf10Sizes[prec] * 8);
    Basics::BigInt::invertSign(numLowerBound, nwdso);
    Basics::BigInt::incrementNN(numLowerBound, nwdso);
}

static bool checkOverflowNN(const void *po, int nwdso, int32 typmodo) 
{
    uint64 numUpperBound[nwdso];
    uint64 numLowerBound[nwdso];
    setNumericBoundsFromType(numUpperBound, numLowerBound, nwdso, typmodo);
    if (Basics::BigInt::compareNN(po, numUpperBound, nwdso) > 0) return false;
    if (Basics::BigInt::compareNN(po, numLowerBound, nwdso) < 0) return false;
    return true;
}

static bool checkOverflowNN(const void *po, int nwo, int nwdso, int32 typmodo) 
{
    if (nwo == nwdso) return checkOverflowNN(po, nwdso, typmodo);

    uint64 numUpperBound[nwdso];
    uint64 numLowerBound[nwdso];
    setNumericBoundsFromType(numUpperBound, numLowerBound, nwdso, typmodo);

    const int64 *w = static_cast<const int64 *>(po);
    Assert(nwo >= nwdso);
    if (Basics::BigInt::isNeg(po, nwo)) {
        for (int i=0; i<nwo-nwdso; ++i) {
            if (w[i] != -1LL) return false; // did not fit in nwdso
        }
        // Check remaining words
        if (Basics::BigInt::ucompareNN(w+nwo-nwdso, numLowerBound, nwdso) < 0)
           return false;
    }
    else {
        for (int i=0; i<nwo-nwdso; ++i) {
            if (w[i] != 0LL) return false; // did not fit in nwdso
        }
        // Check remaining words
        if (Basics::BigInt::ucompareNN(w+nwo-nwdso, numUpperBound, nwdso) > 0)
            return false;
    }
    return true;
}


// From VEval::NumericRescaleDown
static void NumericRescaleDown(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi)
{
    if (isNull(wordsi, nwdsi)) {
        Basics::BigInt::setNull(wordso, nwdso);
        return;
    }

    bool neg = Basics::BigInt::isNeg(wordsi, nwdsi); // work with positive value
    const void *pva = wordsi;
    uint64 wdsa[nwdsi];
    if (neg) {
        Basics::BigInt::copy(wdsa, pva, nwdsi);
        Basics::BigInt::invertSign(wdsa, nwdsi);
        pva = wdsa;
    }

    int scale = Basics::getNumericScale(typmodi) -
        Basics::getNumericScale(typmodo);
    uint64 wdst[nwdsi];
    Basics::BigInt::divUnsignedNN(wdst, nwdsi, 1, NULL, 0, pva, nwdsi, // round
                                  Basics::BigInt::powerOf10Words[scale],
                                  Basics::BigInt::powerOf10Sizes[scale]);

    // Copy to output
    if (nwdsi >= nwdso) {
        // Truncation test
        if (!checkOverflowNN(wdst, nwdsi, nwdso, typmodo))
            ereport(ERROR,
                    (errmsg("Value (%Le) exceeds range of type NUMERIC(%d,%d)", 
                            numericToFloat(wordsi, nwdsi, powl(10.0L, -SCALEFROMTYPMOD(typmodi))),
                            getNumericPrecision(typmodo), getNumericScale(typmodo))));
        Basics::BigInt::copy(wordso, wdst + (nwdsi - nwdso), nwdso);
    }
    else {
        Basics::BigInt::copy(wordso + nwdso - nwdsi, wdst, nwdsi);
        for (int i=0; i<nwdso - nwdsi; ++i) wordso[i] = 0;
    }

    // Restore sign
    if (neg) Basics::BigInt::invertSign(wordso, nwdso); // round towards zero

}

// From VEval::NumericRescaleUp
static void NumericRescaleUp(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi)
{
    int scalei = Basics::getNumericScale(typmodi);
    int scaleo = Basics::getNumericScale(typmodo);

    if (Basics::BigInt::isNull(wordsi, nwdsi))
        Basics::BigInt::setNull(wordso, nwdso);
    else {
        // Work unsigned
        bool neg = Basics::BigInt::isNeg(wordsi, nwdsi);
        const void *pva = wordsi;
        uint64 wdsa[nwdsi];
        if (neg) {
            Basics::BigInt::copy(wdsa, pva, nwdsi);
            Basics::BigInt::invertSign(wdsa, nwdsi);
            pva = wdsa;
        }

        // Multiply up
        int nwdst = nwdsi + Basics::BigInt::powerOf10Sizes[scaleo - scalei];
        uint64 wdst[nwdst];
        Basics::BigInt::mulUnsignedNN(wdst, pva, nwdsi,
                                      Basics::BigInt::powerOf10Words[scaleo - scalei],
                                      Basics::BigInt::powerOf10Sizes[scaleo - scalei]);

        // Copy to output
        if (nwdst >= nwdso) {
            // Truncation test
            if (!checkOverflowNN(wdst, nwdst, nwdso, typmodo)) {
                ereport(ERROR,
                        (errmsg("Value (%Le) exceeds range of type NUMERIC(%d,%d)",
                                numericToFloat(wordsi, nwdsi, powl(10.0L, -SCALEFROMTYPMOD(typmodi))),
                                getNumericPrecision(typmodo), getNumericScale(typmodo))));
            }

            Basics::BigInt::copy(wordso, wdst + nwdst - nwdso, nwdso);
        }
        else {
            Basics::BigInt::copy(wordso + nwdso - nwdst, wdst, nwdst);
            for (int i=0; i<nwdso - nwdst; ++i) wordso[i] = 0;
        }

        // Restore sign
        if (neg) Basics::BigInt::invertSign(wordso, nwdso);
    }
}

// From VEval::NumericRescaleSameScaleSmallerPrec
static void NumericRescaleSameScaleSmallerPrec(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi)
{
    if (isNull(wordsi, nwdsi))
        setNull(wordso, nwdso);
    else
    {
        // Check for truncation
        if (!checkOverflowNN(wordsi, nwdsi, nwdso, typmodo)) {
            ereport(ERROR,
                    (errmsg("Value (%Le) exceeds range of type NUMERIC(%d,%d)",
                            numericToFloat(wordsi, nwdsi, powl(10.0L, -SCALEFROMTYPMOD(typmodi))),
                            getNumericPrecision(typmodo), getNumericScale(typmodo))));
        }
        copy(wordso, wordsi + nwdsi - nwdso, nwdso);
    }
}

/*
 * \brief Cast a numeric from one typmod to another. The params wordsi, nwdsi and
 * typmodi are the words, number of words and typmod of the input numeric,
 * while wordso, nwdso and typmodo are the words, number of words and typmod
 * of output numeric. It is the caller's responsibility to ensure that wordso
 * has space to store 'nwdso' words.
 */
static void castNumeric(uint64 *wordso, int nwdso, int32 typmodo, uint64 *wordsi, int nwdsi, int32 typmodi)
{
    int scalei = Basics::getNumericScale(typmodi);
    int scaleo = Basics::getNumericScale(typmodo);

    // Resize per precision, scale
    if (Basics::isSimilarNumericTypmod(typmodi, typmodo)) {
        VIAssert(nwdsi == nwdso);
        // Could override the non-copy functions...
        if (PRECISIONFROMTYPMOD(typmodi) >
            PRECISIONFROMTYPMOD(typmodo))
        {
            NumericRescaleSameScaleSmallerPrec(wordso, nwdso, typmodo, wordsi, nwdsi, typmodi);
        }
        else
        {
            // Same scale and precision
            copy(wordso, wordsi, nwdsi);
        }
    }
    else if (scalei == scaleo) {
        if (nwdso < nwdsi) {
            // Truncation.
            NumericRescaleSameScaleSmallerPrec(wordso, nwdso, typmodo, wordsi, nwdsi, typmodi);            
        }
        else {
            // Expansion
            // From NumericRescaleSameScaleLargerPrec
            if (isNull(wordsi, nwdsi))
                setNull(wordso, nwdso);
            else {
                // Copy
                copy(wordso + nwdso - nwdsi, wordsi, nwdsi);
                // Sign extend
                uint64 ext = isNeg(wordsi, nwdsi) ? -1LLU : 0;
                uint64 *pout = wordso;
                for (int i=0; i<nwdso - nwdsi; ++i) pout[i] = ext;
            }
        }
    }
    else if (scalei > scaleo)
    {
        NumericRescaleDown(wordso, nwdso, typmodo, wordsi, nwdsi, typmodi);
    }
    else
    {
        NumericRescaleUp(wordso, nwdso, typmodo, wordsi, nwdsi, typmodi);
    }
}


/**
 * x86 only
 * 80-bit extended precision float
 * 64 bit mantissa, stored little endian (just like a 64-bit int)
 *    Note that the mantissa does not have an implicit "1" bit
 *      thus eliminating the concept of denormal numbers.
 * 16 bit exponent, also little endian, within the next 64 bits.
 *    High bit is sign
 *    Lower 15 bits are "excess 16383"
 * Example:
 * 1.0 is stored as, in binary:
 *  mantissa: 1000000000000000000000000000000000000000000000000000000000000000,
 *  exponent: 0011111111111111
 */
union long_double_parts
{
    long_double_parts(long double v = 0.0) : value(v) {}
    long double value;
    struct
    {
        unsigned long long int mantissa;
        unsigned short int exponent; // Includes the sign bit.
        unsigned short int remainder[3];
    };
};

/**
 * \brief Multiply, unsigned only.  uw words by 1 word -> uw+1 words
 *  Output array must be uw+1 words long; may not overlap inputs
 */
static void mulUnsignedN1(void *obuf, const void *ubuf, int uw, uint64 v)
{
    const uint64 *u = static_cast<const uint64 *>(ubuf);
    uint64 *o = static_cast<uint64 *>(obuf);

    Multiply(u[uw-1], v, o[uw-1], o[uw]); // N-by-1 multiply
    for (int i = uw-2; i >= 0; --i) {
        uint64 lo;
        Multiply(u[i], v, o[i], lo);
        o[i+1] += lo;
        if (o[i+1] < lo) o[i]++;        // can't overflow
    }
}

/**
 * \brief Multiply, unsigned only.
 *  Output array must be nw1+nw2 long; may not overlap inputs
 * PERF NOTE: Operates like a school boy, could do Karatsuba (or better)
 */
static void mulUnsignedNN(void *obuf, const void *buf1, int nw1,
                                      const void *buf2, int nw2)
{
    const uint64 *ubuf1 = static_cast<const uint64 *>(buf1);
    const uint64 *ubuf2 = static_cast<const uint64 *>(buf2);
    uint64 *ubufo = static_cast<uint64 *>(obuf);

    int nwo = nw1+nw2;
    setZero(obuf, nwo);

    Assert (nw1>0);
    Assert (nw2>0);

    // Optimize out leading and trailing zeros
    int s1 = 0, e1 = nw1-1;
    for (; (s1 < nw1) && !ubuf1[s1]; ++s1) ;
    for (; (e1 >= 0) && !ubuf1[e1]; --e1) ;

    // Now multiply
    for (int i2=nw2-1; i2 >= 0; --i2)
    {
        if (!ubuf2[i2]) continue; // Optimize out extra 0s

        for (int i1 = e1; i1>= s1; --i1)
        {
            uint64 rl;
            uint64 rh;

            Multiply(ubuf1[i1], ubuf2[i2], rh, rl);

            // Add to the result (w/carry)
            int idx = i1 + i2;
            ubufo[idx+1] += rl;
            if (ubufo[idx+1] < rl) ++rh; // can't overflow
            ubufo[idx] += rh;
            uint64 cf = (ubufo[idx] < rh);

            // Propagate carry
            while (cf) {
                ubufo[--idx] += cf;
                cf = (ubufo[idx] == 0);
            }
        }
    }
}

// qbuf = ubuf / v; r = ubuf % v
//   qbuf is qw words, and ubuf is uw; returns r
// qbuf may be the same as ubuf if qw == uw
// qw may be < uw if the missing result words are zero
static uint64 divUnsignedN1(void *qbuf, int qw, int round,
                                   const void *ubuf, int uw, uint64 v)
{
    if (v == 0) {
        if (isZero(ubuf, uw)) {
            setZero(qbuf, qw);
            return 0;                   // 0/0 -> 0,0
        }
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                        errmsg("division by zero")));
    }
    Assert(qw > 0);
    Assert(uw > 0);
    const uint64 *ub = static_cast<const uint64 *>(ubuf);
    uint64 *qb = static_cast<uint64 *>(qbuf);

    if (qb == ub)
        Assert(qw == uw);
    else
        Basics::BigInt::setZero(qb, qw-uw);

    uint64 rem = 0, q, r;
    for (int j = 0; j < uw; ++j) {
        Divide(rem, ub[j], v, q, r);
        rem = r;
        int k = j + qw - uw;
        if (k < 0)
            Assert(q == 0);
        else
            qb[k] = q;
    }

    if (round && rem >= v - rem) // rem >= v / 2
        Basics::BigInt::incrementNN(qb, qw);
    return rem;
}

// qbuf = ubuf / vbuf; rbuf = ubuf % vbuf
//   qbuf must be qw words; rbuf must be rw words or NULL
//   round: trunc=0, round-up=1
// qbuf may overlay ubuf
// Knuth Vol 2, 4.3.1, Algorithm D, pg 257
static void divUnsignedNN(void *qbuf, int qw, int round,
                                 uint64 *rbuf, int rw, const void *ubuf,
                                 int uw, const void *vbuf, int vw)
{
    Assert(qw > 0);
    Assert(uw > 0);
    Assert(vw > 0);

    if (rbuf)
        Basics::BigInt::setZero(rbuf, rw);

    // Optimize out leading zeros from v
    const uint64 *vb = static_cast<const uint64 *>(vbuf);
    int vs = 0;
    while (vb[vs] == 0)
        if (++vs == vw) {
            if (isZero(ubuf, uw)) {
                setZero(qbuf, qw);
                return;                 // 0/0 -> 0,0
            }
            ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO),
                            errmsg("division by zero")));
        }
    int n = vw - vs;                    // length of v in words

    // Optimize out leading zeros from u
    const uint64 *ub = static_cast<const uint64 *>(ubuf);
    int us = 0;
    while (ub[us] == 0)
        if (++us == uw) {
            Basics::BigInt::setZero(qbuf, qw);
            return;                     // u == 0, so qbuf and rbuf = 0
        }
    int ul = uw - us;                   // length of u in words (m+n)

    uint64 *qb = static_cast<uint64 *>(qbuf);

    // Optimize N by 1 case
    if (n == 1) {
        uint64 rem = divUnsignedN1(qb, qw, round, ub+us, ul, vb[vw-1]);
        if (rbuf)
            rbuf[rw-1] = rem;
        return;
    }

    // D1 Normalize
    uint64 q, r;
    uint64 v[n+2];                      // v is v[1..n] scaled by d
    v[0] = v[n+1] = 0;                  // v[n+1] is a hack; verify if it works!
    Basics::BigInt::copy(v+1, vb+vs, n);
    uint64 d = __builtin_clzll(v[1]);   // count leading zero bits
    Basics::BigInt::shiftLeftNN(v+1, n, d);

    int m = ul - n;

    uint64 u[ul+1];                     // u is u[0..ul] scaled by d
    u[0] = 0;
    Basics::BigInt::copy(u+1, ub+us, ul);
    Basics::BigInt::shiftLeftNN(u, ul+1, d);

    Basics::BigInt::setZero(qb, qw);

    // D2 Loop for each q digit
    for (int j = 0; j <= m; ++j) {
        // D3 calculate qhat and rhat
        if (u[j] == v[1]) {             // Divide will overflow
            q = 0;
            r = u[j+1];
            goto tag4H;
        }
        Divide(u[j],u[j+1], v[1], q, r);

        while (true) {
            uint64 hi, lo;
            Multiply(v[2], q, hi, lo);  // hack if vl==1
            if (hi < r ||
                (hi == r && lo <= u[j+2]))
                break;                  // q is no more than 1 too high
        tag4H:
            --q;
            r += v[1];
            if (r < v[1])               // 056: overflow, quit
                break;
        }

        uint64 t[n+1];                  // D4 multiply and subtract
        mulUnsignedN1(t, v+1, n, q);
        Basics::BigInt::subNN(u+j, u+j, t, n+1);

        if (u[j] != 0) {                // D5 test remainder
            Basics::BigInt::addNN(u+j, u+j, v, n+1); // D6 add back
            --q;
            Assert(u[j] == 0);
        }
        if (q != 0) {
            int k = j - m + qw - 1;
            Assert(k >= 0 && k < qw);
            qb[k] = q;
        }
    }

    if (rbuf) {
        int i = Min(ul, n);
        Assert(rw >= i);                              // rbuf overflow
        Basics::BigInt::copy(rbuf+rw-i, u+ul+1-i, i); // D8 unnormalize
        Basics::BigInt::shiftRightNN(rbuf+rw-i, i, d);
    }

    if (round && m >= 0) {
        Basics::BigInt::shiftLeftNN(u+m, n+1, 1);        // double r
        if (Basics::BigInt::compareNN(u+m, v, n+1) >= 0) // if r >= v/2
            Basics::BigInt::incrementNN(qb, qw);
    }
    return;
}

/**
 * \brief Rescale a given numeric to a specific prec/scale/nwds
 * The input should have minimal precision to avoid unnecessary overflow;
 * for example, "0" should have precision 0 (as generated by charToNumeric).
 * Accepts signed numerics.  Will set its "in" argument to abs(in).
 */
static bool rescaleNumeric(void *out, int ow, int pout, int sout,
                           void *in, int iw, int pin, int sin)
{
    // Adjust the scale as necessary
    int adj = sin - sout;               // if adj > 0, round
    if (pin - adj > pout)
        return false;                   // will not fit

    uint64 *ob = static_cast<uint64 *>(out);
    bool neg = isNeg(in, iw);
    if (neg) invertSign(in, iw);
    if (adj == 0) {
        Assert(ow >= iw);
        copy(ob + ow - iw, in, iw);
    }
    else if (adj < 0) {                 // increase the scale
        int size = BigInt::powerOf10Sizes[-adj];
        int nwdsprod = iw + size;
        uint64 prod[nwdsprod];
        mulUnsignedNN(prod, in, iw, BigInt::powerOf10Words[-adj], size);
        iw = Min(nwdsprod, ow);         // non-zero prod should fit into out
        copy(ob + ow - iw, prod + nwdsprod - iw, iw);
    } else {
        divUnsignedNN(ob, ow, 1, NULL, 0, in, iw, // round
                      BigInt::powerOf10Words[adj], BigInt::powerOf10Sizes[adj]);
        iw = ow;
    }

    for (int i = 0; i < ow - iw; ++i) ob[i] = 0;
    if (neg) invertSign(ob, ow);

    return true;
}

/**
 * \brief Convert floating point multiplied by 10^scale to an integer
 *  This truncates if round is false; otherwise the numeric result is rounded
 *  @return false if high bits were lost, true if reasonable fidelity was achieved
 */
static bool setFromFloat(void *bbuf, int bwords,
                                const FiveToScale &fiveToScale,
                                long double value, bool round)
{
    long_double_parts ldp(value);

    int exp = ldp.exponent;
    bool neg = (exp & 0x8000) != 0;
    exp &= 0x7fff;

    // Check for Bad Things (infinity, NaN, etc.)
    if (exp == 0x7fff) {
        vfloat v = value;
        if (vfloatIsNull(&v)) {
            setNull(bbuf, bwords);
            return true;
        }
        else {
            //NaN, or some other garbage we can't make NUMERIC
            return false;
        }
    }

    // Check for zero.
    if (exp == 0 && ldp.mantissa == 0) {
        setZero(bbuf, bwords);
        return true;
    }
    exp -= 16383;

    int bitsToShift = fiveToScale.scale + exp - 63;

    // Setup intermediate value with enough space to multiply the mantissa, first by
    // 5^scale and then by 2^(scale+exp-63).
    int shiftWords = (abs(bitsToShift)+63)/64;
    int nw = 1 + fiveToScale.wordCount + shiftWords;
    uint64 buf[nw];

    if (bitsToShift >= 0) {
        // Multiply.
        mulUnsignedN1(buf+shiftWords, fiveToScale.value, fiveToScale.wordCount, ldp.mantissa);

        // Shift to appropriate position.
        if (shiftWords != 0)
            buf[shiftWords-1] = 0;      // clear a word of high bits
        shiftLeftNN(buf, nw, bitsToShift);
    } else {
        // Multiply.
        mulUnsignedN1(buf, fiveToScale.value, fiveToScale.wordCount, ldp.mantissa);

        // Shift to appropriate position.
        buf[nw - shiftWords] = 0;       // VER-18339 Valgrind false positive
        shiftRightNN(buf, nw, -bitsToShift);
        nw -= shiftWords;

        if (round && int64(buf[nw]) < 0)
            incrementNN(buf, nw);
    }

    // Check overflow.
    int j=0;
    for ( ; j<nw-bwords; ++j) {
        if (buf[j]) return false;
    }
    // Copy to output.
    uint64 *result = static_cast<uint64 *>(bbuf);
    int i=0;
    for ( ; i<bwords-nw; ++i) {
        result[i] = 0;
    }
    for ( ; i<bwords; ++i, ++j) {
        result[i] = buf[j];
    }

    // Make negative if needed
    if (neg) invertSign(bbuf, bwords);

    return true;
}

/**
 * \brief Convert Numeric to a float helper function
 * Input should not be negative / null
 */
static long double convertPosToFloatNN(const void *bbuf, int bwords)
{
    const uint64 *buf = static_cast<const uint64 *>(bbuf);

    // Count leading zero bits
    int zbits = 0;
    int i; // Will index the first nonzero word
    for (i=0; i<bwords; ++i) {
        if (buf[i] != 0) {
            zbits += __builtin_clzll(buf[i]);
            break;
        }
        zbits += 64;
    }
    int tbits = bwords * 64;
    if (zbits == tbits) return 0.0L; // Is zero, let's not confuse ourselves.

    // Pick out mantissa
    uint64 m = buf[i] << (zbits & 63);
    ++i;
    if ((zbits & 63) && i != bwords) {
         m |= (buf[i]) >> (64 - (zbits & 63));
    }

    // Build resulting float
    long_double_parts ldp;
    ldp.mantissa = m;
    ldp.exponent = tbits - zbits + 16382; // 1 nonzero bit = 2^0 = 16383 exponent

    return ldp.value;
}

/**
 * \brief Convert Numeric to a float
 * Handles negative / null input
 *
 * @param tenthtoscale 10^-scale of the Numeric
 */
static ifloat numericToFloat(const void *buf, int nwds, ifloat tenthtoscale)
{
    if (Basics::BigInt::isNull(buf, nwds)) {
        return vfloat_null;
    }
    else {
        bool isNeg = false;
        uint64 tmp[nwds];
        if (Basics::BigInt::isNeg(buf, nwds)) {
            Basics::BigInt::copy(tmp, buf, nwds);
            Basics::BigInt::invertSign(tmp, nwds);
            buf = tmp;
            isNeg = true;
        }
        long double rv = Basics::BigInt::convertPosToFloatNN(buf, nwds)
            * tenthtoscale;
        return isNeg ? -rv : rv;
    }
}
/**
 * \brief Multiply Numerics , result in outNum
 * Handles negative / null input
 */
static void numericMultiply(const uint64 *pa, int nwdsa,
                            const uint64 *pb, int nwdsb,
                            uint64 *outNum, int nwdso)
{
    if (nwdsa + nwdsb == nwdso)
    {
        if (Basics::BigInt::isNull(pa, nwdsa) ||
            Basics::BigInt::isNull(pb, nwdsb))
        {
            Basics::BigInt::setNull(outNum, nwdso);
        }
        else
        {
            // Multiply unsigned
            uint64 wdsa[nwdsa], wdsb[nwdsb]; // For sign inversions if needed
            const void *pva = pa, *pvb = pb;
            int negs = 0;
            if (Basics::BigInt::isNeg(pa, nwdsa)) {
                ++negs;
                pva = wdsa;
                Basics::BigInt::copy(wdsa, pa, nwdsa);
                Basics::BigInt::invertSign(wdsa, nwdsa);
            }
            if (Basics::BigInt::isNeg(pb, nwdsb)) {
                ++negs;
                pvb = wdsb;
                Basics::BigInt::copy(wdsb, pb, nwdsb);
                Basics::BigInt::invertSign(wdsb, nwdsb);
            }
            Basics::BigInt::mulUnsignedNN(outNum, pva, nwdsa, pvb, nwdsb);
            if (negs == 1) Basics::BigInt::invertSign(outNum, nwdso);
        }
    }
    else
    {
        if (Basics::BigInt::isNull(pa, nwdsa) ||
            Basics::BigInt::isNull(pb, nwdsb))
        {
            Basics::BigInt::setNull(outNum, nwdso);
        }
        else
        {
            // Multiply unsigned...
            uint64 otemp[nwdsa+nwdsb]; // Result has to go in temp place
            uint64 wdsa[nwdsa], wdsb[nwdsb]; // For sign inversions if needed
            const void *pva = pa, *pvb = pb;
            int negs = 0;
            if (Basics::BigInt::isNeg(pa, nwdsa)) {
                ++negs;
                pva = wdsa;
                Basics::BigInt::copy(wdsa, pa, nwdsa);
                Basics::BigInt::invertSign(wdsa, nwdsa);
            }
            if (Basics::BigInt::isNeg(pb, nwdsb)) {
                ++negs;
                pvb = wdsb;
                Basics::BigInt::copy(wdsb, pb, nwdsb);
                Basics::BigInt::invertSign(wdsb, nwdsb);
            }
            Basics::BigInt::mulUnsignedNN(otemp, pva, nwdsa, pvb, nwdsb);
            Basics::BigInt::copy(outNum, otemp+(nwdsa+nwdsb-nwdso), nwdso);
            if (negs == 1) Basics::BigInt::invertSign(outNum, nwdso);
        }
    }
}
/**
 * \brief Divide Numerics
 * Handles negative / null input
 */
static void numericDivide(const uint64 *pa, int nwdsa, int32 typmoda,
                          const uint64 *pb, int nwdsb, int32 typmodb,
                          uint64 *outNum, int nwdso, int32 typmodo)
{
    if (Basics::BigInt::isNull(pa, nwdsa) ||
        Basics::BigInt::isNull(pb, nwdsb))
    {
        Basics::BigInt::setNull(outNum, nwdso);
        return;
    }

    uint64 wdsa[nwdsa], wdsb[nwdsb]; // For sign inversions if needed
    const uint64 *pva = pa, *pvb = pb;
    int negs = 0;
    if (Basics::BigInt::isNeg(pva, nwdsa)) {
        ++negs;
        Basics::BigInt::copy(wdsa, pva, nwdsa);
        Basics::BigInt::invertSign(wdsa, nwdsa);
        pva = wdsa;
    }
    if (Basics::BigInt::isNeg(pvb, nwdsb)) {
        ++negs;
        Basics::BigInt::copy(wdsb, pvb, nwdsb);
        Basics::BigInt::invertSign(wdsb, nwdsb);
        pvb = wdsb;
    }

    // Scale numerator (pa) if necessary
    int scale = SCALEFROMTYPMOD(typmodb) - SCALEFROMTYPMOD(typmoda) + SCALEFROMTYPMOD(typmodo);
    int nwdsp = Basics::BigInt::powerOf10Sizes[scale];
    int nwdss = nwdsa;
    uint64 wdss[nwdss + nwdsp];
    if (scale != 0) {
        Assert(scale > 0);
        nwdss = nwdss + nwdsp;
        Basics::BigInt::mulUnsignedNN(wdss, pva, nwdsa,
                                      Basics::BigInt::powerOf10Words[scale],
                                      nwdsp);
        pva = wdss;
    }

    Basics::BigInt::divUnsignedNN(outNum, nwdso, 1, NULL, 0,
                                  pva, nwdss, pvb, nwdsb); // round
    if (negs == 1) Basics::BigInt::invertSign(outNum, nwdso);
}


/*
 * @cond INTERNAL Declaration of rest of BigInt member functions,
 * used internally in Vertica, definition not exposed to UDx writers
 */
#define InlineNNSubs 0 // 0 for debugging, 1 for inlining

#if InlineNNSubs

#define STATIC static
#define MEMBER
#include "Basics/BigIntSubs.h"
#undef MEMBER
#undef STATIC

#else

static void accumulateN1(void *obuf, int ow, uint64 v);

static bool charToNumeric(const char *c, int clen, bool allowE,
                          int64 *&pout, int &outWords,
                          int &precis, int &scale);

static void binaryToDec(const void *bbuf, int bwords, char *outBuf, int olen);

static void binaryToDecScale(const void *bbuf, int bwords, char *outBuf,
                             int olen, int scale);

#endif
/* @endcond */

};

/* @cond INTERNAL */
// Call the destructor on T
template <class T> void destr(void *t) {
    static_cast<T *>(t)->~T();
};
/* @endcond */

}

/** 
 * \def talloc(_pool, _type)
 * \brief Allocates a block of memory from a thread-local sub-pool
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of memory (struct, vint, char, etc.) to be allocated
 * @return a pointer to cleared space the size of _type
 */
#define talloc(_pool, _type)                                 \
    (static_cast<_type *>((_pool)->alloc(sizeof(_type))))

/** 
 * \def tallocSize(_pool, _type, _size)
 * \brief Allocates a specific size block of memory from a thread-local sub-pool
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of memory (struct, void, vint, etc.) to be allocated
 * @param _size the size of this variable-sized struct
 * @return a pointer to cleared space the size of _type
 */
#define tallocSize(_pool, _type, _size)                      \
    (static_cast<_type *>((_pool)->alloc(_size)))

/**
* \brief Allocates an array of memory blocks from a thread-local sub-pool
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of memory (struct, vint, char, etc.) to be allocated
 * @param _len the number of blocks in the array (packed tightly)
 * @return a pointer to a cleared array
 */
#define tallocArray(_pool, _type, _len)                      \
    (static_cast<_type *>((_pool)->alloc(sizeof(_type) * (_len))))

/** \brief Constructs an object from a thread-local sub-pool
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of the object to be constructed
 * @param _args optional list of arguments to the constructor
 * @return a pointer to a cleared, then constructed object
 */
#define tnew(_pool, _type, _args...)                                    \
    (static_cast<_type *>((_pool)->constrD(                             \
        new ((_pool)->constrA(sizeof(_type))) _type(_args),             \
        Basics::destr<_type>)))

/** \brief Similar to tnew(), except when the _type is a class template, the evilness of
 * macro doesn't let us easily pass template arguments with tnew
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of the object to be constructed
 * @param _targ1 the first template argument to the class template
 * @param _targ2 the second template argument to the class template
 * @param _targ3 the second template argument to the class template
 * @param _args optional list of arguments to the constructor
 * @return a pointer to a cleared, then constructed object
 */
#define tnew1TemplateArg(_pool, _type, _targ1, _args...)                \
    (static_cast<_type<_targ1> *>((_pool)->constrD(                     \
        new ((_pool)->constrA(sizeof(_type<_targ1>)))                   \
        _type<_targ1>(_args),                                           \
        Basics::destr<_type<_targ1> >)))

#define tnew2TemplateArg(_pool, _type, _targ1, _targ2, _args...)        \
    (static_cast<_type<_targ1, _targ2> *>((_pool)->constrD(            \
        new ((_pool)->constrA(sizeof(_type<_targ1, _targ2>)))           \
        _type<_targ1, _targ2>(_args),                                   \
        Basics::destr<_type<_targ1, _targ2> >)))

#define tnew3TemplateArg(_pool, _type, _targ1, _targ2, _targ3, _args...) \
    (static_cast<_type<_targ1, _targ2, _targ3> *>((_pool)->constrD(     \
        new ((_pool)->constrA(sizeof(_type<_targ1, _targ2, _targ3>)))    \
        _type<_targ1, _targ2, _targ3>(_args),                            \
        Basics::destr<_type<_targ1, _targ2, _targ3> >)))

/** \brief Constructs an array of objects from a thread-local sub-pool
 * @param _pool a pointer to a TaskPool instance
 * @param _type the type of the objects to be constructed
 * @param _len the number of objects in the array
 * @return a pointer to a cleared, then constructed array of objects
 */
#define tnewArray(_pool, _type, _len)                                   \
    (static_cast<_type *>((_pool)->constrD(                             \
        new ((_pool)->constrA(sizeof(_type), (_len))) _type[_len],      \
        Basics::destr<_type>)))


/** Allocates a block of memory to fit a specific data type (vint, struct, etc.).
 *  
 */
#define vt_alloc(_allocator, _type) talloc(_allocator, _type)

/** Allocates a block of memory to hold an array of a specific data type.
 *  
 */
#define vt_allocSize(_allocator, _type, _size) tallocSize(_allocator, _type, _size)

/** Allocates an arbitrarilty-sized block of memory.
 *  
 */

#define vt_allocArray(_allocator, _type, _len) tallocArray(_allocator, _type, _len)

// Only supposed to be used to create UDScalarFuncObj in UDScalarFuncInfo::createFuncObj()
#define vt_createFuncObj(_allocator, _type, _args...)                         \
    (new ((_allocator)->alloc(sizeof(_type))) _type(_args))

#endif // BASICS_UDX_SHARED_H
