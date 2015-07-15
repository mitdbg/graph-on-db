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
#ifndef BASICSSUPPORT_H_
#define BASICSSUPPORT_H_

/*
 * \cond INTERNAL
 * Returns the largest length such that all src_len - length characters to the
 * right are the specified character
 */
inline int32
ScanFromRight(const char *src, int32 alen, char chr)
{
    while (alen > 0 && src[alen-1] == chr) --alen;
    return alen;
}
/*
 * \cond INTERNAL
 *
 * Returns the largest length such that all src_len - length characters to the
 * right return true
 */
inline int32
ScanFromRight(const char *src, int32 alen, int (*fn)(int c))
{
    while (alen > 0 && fn(src[alen-1])) --alen;
    return alen;
}
/*
 * \cond INTERNAL
 *
 * Returns the largest length such the each of the first length characters are
 * the specified character
 */
inline int32
ScanFromLeft(const char *src, int32 src_len, char chr)
{
    int32 alen = 0;
    while (alen < src_len && src[alen] == chr) ++alen;
    return alen;
}
/*
 * \cond INTERNAL
 */
inline int32
ScanFromLeft(const char *src, int32 src_len, int (*fn)(int c))
{
    int32 alen = 0;
    while (alen < src_len && fn(src[alen])) ++alen;
    return alen;
}
/*
 * \cond INTERNAL
 */
inline void vsnulcpy(void *dst, const void *src, size_t len) {
    vsmemcpy(dst, src, len);
    reinterpret_cast<char *>(dst)[len] = '\0';
}

#endif /* BASICSSUPPORT_H_ */
