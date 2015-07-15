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
 * Create Date: Mar 24, 2008 
 */

#ifndef BASICS_OIDS
#define BASICS_OIDS

// This file is intended for export to Vertica client applications

/// The Postgres types understood by Vertica I/O, e.g. Load.cpp and Root.cpp
/// See PG/src/include/catalog/pg_type.h, which must match
// must match const char* OidName[] in Type.cpp

typedef unsigned long long int BaseDataOID;

#define VUnspecOID       ((BaseDataOID) 0)              // Vertica: Unspecified
#define VTuple           ((BaseDataOID) 1)              // Vertica: tuple
#define VPosOID         ((BaseDataOID) 2)              // Vertica Position
#define RecordOID      ((BaseDataOID) 3)              // PG record, was 2249
#define UnknownOID      ((BaseDataOID) 4)              // T_String, was 705
#define BoolOID         ((BaseDataOID) 5)            // was 16
#define Int8OID         ((BaseDataOID) 6)              // was 20
#define Float8OID       ((BaseDataOID) 7)              // was 701
#define CharOID         ((BaseDataOID) 8)              // BPCHAROID, was 1042
#define VarcharOID      ((BaseDataOID) 9)              // was 1043
#define DateOID         ((BaseDataOID) 10)             // was 1082
#define TimeOID         ((BaseDataOID) 11)             // was 1083
#define TimestampOID    ((BaseDataOID) 12)             // was 1114
#define TimestampTzOID  ((BaseDataOID) 13)             // was 1184
#define IntervalOID     ((BaseDataOID) 14)             // was 1186
#define IntervalYMOID   ((BaseDataOID) 114)            // year-month interval
#define TimeTzOID       ((BaseDataOID) 15)             // was 1266
#define NumericOID      ((BaseDataOID) 16)             // was 1700
#define VarbinaryOID    ((BaseDataOID) 17)             // equivalent to PG ByteaOID
#define RLETuple      ((BaseDataOID) 18)             // Vertica: RLE_count/Tuple pairs
#define BinaryOID       ((BaseDataOID) 117)            // binary type (see TypeData definition
#define LongVarbinaryOID ((BaseDataOID) 116)           // The Long* Oids are chosen mostly arbitrarily,
#define LongVarcharOID ((BaseDataOID) 115)             // to be near BinaryOID and not conflict with anything.

// To support polymorphic functions
#define ANYOID			2276

#endif //BASICS_OIDS
