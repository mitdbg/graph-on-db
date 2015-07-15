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
 * Description: Support code for VNumerics
 *
 * Create Date: Feb 27, 2012
 */

#include "BasicsSupport.h"

/*
 * \cond INTERNAL
 */
static bool read_scales(const char *cp, int clen, int *dscale, int *bscale)
{
    char buf[clen+1];
    vsnulcpy(buf, cp, clen);            // allow the use of strtol
    char *c = buf;
    if (*c == 'p' || *c == 'P')
        *bscale = strtol(c+1, &c, 10);  // just a simple 'p' is legal, like p0
    if (*c == 'e' || *c == 'E')
        *dscale = strtol(c+1, &c, 10);  // just a simple 'e' is legal, like e0
    return *c == '\0';
}

namespace Basics {

/*
 * Conversion of a character string to numeric
 * outWords is an in/out argument (to prevent overflow)
 * format is <space>* [+-]? <number> [p[[+-]<int>]] [e[+-]<int>] <space>*
 * <number> is [0-9_]*]\.?[0-9_]* or 0x[0-9a-f_]*\.?[0-9a-f_]*
 * <number> must have at least one digit; for hex, p must preceed e
 * The e-only base syntax is accepted only for decimal if allowE is true.
 * Returns false if input is invalid or will not fit in output.
 * Uses the minimal value for precis which is >= scale, so "0" has precis 0;
 * callers may need to adjust that to precis 1.
 * The original pout area contains the result, but pout and ow are reset to
 * the part of the result corresponding to precis.
 */
bool BigInt::charToNumeric(const char *c, int clen, bool allowE,
                                 int64 *&out, int &outWords,
                                 int &precis, int &scale)
{
    int ow = 0;
    int n = ScanFromLeft(c, clen, ' ');
    c += n; clen -= n;
    clen = ScanFromRight(c, clen, ' ');

    if (clen == 0)
        return false;                   // empty string is invalid
    bool neg = (c[0] == '-');
    if (neg || c[0] == '+')
        ++c, --clen;
    int dscale = 0, bscale = 0, digits = 0, period = 0, places = 0, lzo = 0;
    uint64 word = 0, d, wk[outWords+1];

    if (clen > 2 && c[0] == '0' && (c[1] == 'x' || c[1] == 'X')) {
        // convert to hex
        c += 2; clen -= 2;
        lzo = ScanFromLeft(c, clen, '0'); // ignore leading zeros
        c += lzo; clen -= lzo;
        for ( ; clen > 0; ++c, --clen) {
            switch (c[0]) {
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                d = c[0] - '0'; break;
            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
                d = c[0] - 'a' + 10; break;
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
                d = c[0] - 'A' + 10; break;
            case '_':
                continue;
            case '.':
                if (period++)
                    return false;       // multiple periods
                places = digits;        // integer places
                continue;
            case 'p': case 'P':
                if (!read_scales(c, clen, &dscale, &bscale))
                    return false;       // scale syntax error
                clen = 0;               // done
                continue;
            default:
                return false;           // invalid character
            }
            word = (word << 4) | d;
            ++digits;
            if (digits % 16 == 0) {
                if (ow == outWords)
                    return false;       // overflow
                out[ow++] = word;
                word = 0;
            }
        }
        n = digits % 16;
        if (n != 0) {
            if (ow == outWords)
                return false;       // overflow
            out[ow] = 0;
            shiftRightNN(out, ow+1, (16 - n) * 4);
            out[ow++] |= word;
        }

        if (period)
            bscale -= (digits - places) * 4;
        precis = int(digits * 1.20412 + 0.99999); // log10(16)
        scale = -dscale;
    }
    else {
        // convert to decimal
        lzo = ScanFromLeft(c, clen, '0'); // ignore leading zeros
        c += lzo; clen -= lzo;
        for ( ; clen > 0; ++c, --clen) {
            switch (c[0]) {
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                d = c[0] - '0'; break;
            case '_':
                continue;
            case '.':
                if (period++)
                    return false;       // multiple periods
                places = digits;        // integer places
                continue;
            case 'e': case 'E':
                if (!allowE)
                    return false;
                /* FALLTHRU */
            case 'p': case 'P':
                if (!read_scales(c, clen, &dscale, &bscale))
                    return false;       // scale syntax error
                clen = 0;               // done
                continue;
            default:
                return false;           // invalid character
            }
            word = (word * 10) + d;
            ++digits;
            if (digits % 19 == 0) {
                if (ow == 0) {
                    out[ow++] = word;   // first word
                } else {
                    if (ow == outWords)
                        return false;   // overflow
                    mulUnsignedN1(wk, out, ow, *powerOf10Words[19]);
                    ++ow;
                    accumulateN1(wk, ow, word);
                    copy(out, wk, ow);
                }
                word = 0;
            }
        }
        n = digits % 19;
        if (n != 0) {
            if (ow == 0)
                out[ow++] = word;   // first and only word
            else {
                if (ow == outWords)
                    return false;   // overflow
                mulUnsignedN1(wk, out, ow, *powerOf10Words[n]);
                ++ow;
                accumulateN1(wk, ow, word);
                copy(out, wk, ow);
            }
        }

        precis = digits;
        scale = period ? digits - places - dscale : -dscale;
    }

    if (digits == 0 && lzo == 0)
        return false;                   // no digits at all

    if (scale < 0) {                    // convert to scale 0
        if (uint(-scale) >= sizeof(BigInt::powerOf10Sizes) / sizeof(BigInt::powerOf10Sizes[0]))
            return false;               // decimal scale is too big
        int size = Basics::BigInt::powerOf10Sizes[-scale];
        uint64 prod[ow + size];
        mulUnsignedNN(prod, out, ow,
                      Basics::BigInt::powerOf10Words[-scale], size);
        ow += size;
        if (ow > outWords)
            return false;               // overflowed
        copy(out, prod, ow);
        precis += -scale;
        scale = 0;
    } else if (scale > precis)
        precis = scale;                 // an adjustment is done later

    if (bscale > 0) {
        n = (bscale + 63) / 64;         // additional int64 words required
        if (ow + n > outWords)
            return false;               // overflow
        vsmemclr(out + ow, n * sizeof(int64));
        shiftRightNN(out, ow+1, kMod(-bscale, 64));
        ow += n;
        precis += int(bscale * 0.30103 + 1.0); // log10(2)
    }
    else if (bscale < 0) {
        if (uint(-bscale) >=
            sizeof(Basics::fiveToScales)/sizeof(Basics::fiveToScales[0]))
            return false;               // decimal scale is too big
        int size = Basics::fiveToScales[-bscale].wordCount;
        uint64 prod[ow + size];
        mulUnsignedNN(prod, out, ow,
                      Basics::fiveToScales[-bscale].value, size);
        ow += size;
        copy(out, prod, ow);
        scale += -bscale;
        precis += -bscale;              // usually more than necessary
    }

    // Tighten up the precision for the hex and binary scaling cases
    int w = ow; int64 *o = out;
    while (w > 0 && *o == 0) --w, ++o;  // skip leading zero words, if any
    while (precis > scale) {
        if (precis > int(sizeof(powerOf10Sizes) / sizeof(*powerOf10Sizes)))
            return false;
        n = powerOf10Sizes[precis-1];
        if (w > n)
            break;                      // o has more words than 10^(precis-1)
        if (w == n && ucompareNN(o, powerOf10Words[precis-1], n) >= 0)
            break;
        --precis;
    }

    // limit the input precision
    if (precis > NUMERIC_MAX_PRECISION)
        return false;

    // Adjust result size if necessary; automatically allows room for sign bit
    n = Basics::getNumericWordCount(precis);
    int adj = n - ow;
    if (adj > 0) {
        if (n > outWords)
            return false;               // overflowed
        memmove(out + adj, out, ow * sizeof(int64));
        vsmemclr(out, adj * sizeof(int64));
    } else if (adj < 0)
        out -= adj;
    outWords = n;

    // Make negative as needed
    if (neg)
        invertSign(out, n);

    return true;
}

/*
 * \cond INTERNAL
 */
void BigInt::accumulateN1(void *obuf, int ow, uint64 v)
{
    uint64 *o = static_cast<uint64 *>(obuf);
    while (v != 0 && ow > 0) {
        o[--ow] += v;
        v = (o[ow] < v);
    }
}
/**
 * \cond INTERNAL
 *
 * Conversion of binary to dec (no leading 0s, but trailing 0s)
 * char sizing guideline: must be greater than or equal to 20*(# of words) + 2
 */
void BigInt::binaryToDec(const void *bbuf, int bwords, char *outBuf, int olen)
{
    Assert(olen >= 2);
    uint64 curval[bwords];
    copy(curval, bbuf, bwords);

    // We will work on positive values, so if negative invert now
    if (isNeg(curval, bwords)) {
        outBuf[0] = '-'; ++outBuf; --olen;
        *outBuf = 0;
        invertSign(curval, bwords);
    }
    int used = usedWordsUnsigned(curval, bwords);
    if (used == 0) used = 1;

    // OK, time to get serious.  We will work in sets of 19 digits,
    //  the number of sets is bwords * 20 / 19 + 1.  Last is unscaled.
    uint64 rmdr[20 * used / 19 + 1];
    int r = 0;
    do {
        rmdr[r++] = divUnsignedN1(curval + bwords - used, used, false,
                                  curval + bwords - used, used,
                                  *powerOf10Words[19]);
        if (curval[bwords - used] == 0) --used;
    } while (used);
    int n = snprintf(outBuf, olen, "%llu", rmdr[--r]);
    while (true) {
        outBuf += n; olen -= n;
        if (olen <= 0)
            ereport(INTERNAL, (errmsg("Numeric output buffer too small"),
                               errcode(ERRCODE_INTERNAL_ERROR)));
        if (r == 0)
            break;
        n = snprintf(outBuf, olen, "%019llu", rmdr[--r]);
    }
}

/**
 * \cond INTERNAL
 *
 * Conversion of binary to dec, given num digits after decimal point
 *  char sizing guideline: must be greater or equal to 20*(# of words) + 4
 */
void BigInt::binaryToDecScale(const void *bbuf, int bwords, char *outBuf,
                                    int olen, int scale)
{
    // Make decimal
    binaryToDec(bbuf, bwords, outBuf, olen-2); // reserve space for '0.'
    if (scale == 0)
        return;                         // no fraction digits

    if (outBuf[0] == '-')
        ++outBuf;                       // skip minus sign
    int precis = strlen(outBuf);
    int n = precis - scale;
    if (n > 0) {
        memmove(outBuf+n+1, outBuf+n, scale+1); // include NIL
        outBuf[n] = '.';
        return;
    }
    memmove(outBuf+2-n, outBuf, precis+1); // include NIL
    memset(outBuf, '0', 2-n);
    outBuf[1] = '.';
}


} // namespace Basics

// Defined outside the class to get the dependency order right with VerticaType
bool VNumeric::charToNumeric(char *str, const VerticaType &type, VNumeric &target) {
    uint64 *tmpStorage = (uint64*)alloca(target.nwds*8 * 2); // Extra factor of 2 just for good measure
    VNumeric tmpVal(tmpStorage, target.typmod);
    int64 *pout = (int64*) tmpVal.words;

    int wds = sizeof(tmpStorage) / sizeof(tmpStorage[0]);
    int p, s;
    if (!Basics::BigInt::charToNumeric(str, strlen(str), true,
            pout, wds, p, s)) {
        return false;
    }
    if (!Basics::BigInt::rescaleNumeric(target.words,
            type.getNumericLength() >> 3, Basics::getNumericPrecision(
                    target.typmod), Basics::getNumericScale(target.typmod),
            pout, wds, p, s)) {
        return false;
    }

    return true;
}
