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
#ifndef VERTICA_UDX_H
#define VERTICA_UDX_H

/*
 * Include definitions shared between UDx and Vertica server
 */

/** \internal @file
 * \brief Contains the classes needed to write User-Defined things in Vertica
 *
 * This file should not be directly #include'd in user code. Include 'Vertica.h' instead
 */

typedef void (*VT_THROW_EXCEPTION)(int errcode, const std::string &message, const char *filename, int lineno);
extern "C" VT_THROW_EXCEPTION vt_throw_exception;
extern int makeErrMsg(std::stringstream &err_msg, const char *fmt, ...);
extern void dummy();

#include "PGUDxShared.h"
#include "BasicsUDxShared.h"
#include "EEUDxShared.h"
#include "TimestampUDxShared.h"
#include "IntervalUDx.h"

extern Oid getUDTypeOid(const char *typeName);
extern Oid getUDTypeUnderlyingOid(Oid typeOid);
extern bool isUDType(Oid typeOid);

class UdfSupport; // fwd declaration for Vertica internal class
class UDxSideProcessInfo;
class ServerInterface;
class VerticaBlockSerializer;
class BlockReader;
class BlockWriter;
class PartitionReader;
class PartitionWriter;
class AnalyticPartitionReader;
class AnalyticPartitionWriter;
class AnalyticPartitionReaderHelper;
class VerticaRInterface;
class FullPartition;
class IntermediateAggs;
class MultipleIntermediateAggs;
class ParamReader;
class ParamWriter;
class VerticaType;

struct VerticaBuildInfo
{
    const char *brand_name;
    const char *brand_version;
    const char *codename;
    const char *build_date;
    const char *build_machine;
    const char *branch;
    const char *revision;
    const char *checksum;

    VerticaBuildInfo(): 
        brand_name(NULL), brand_version(NULL),
        codename(NULL), build_date(NULL),
        build_machine(NULL), branch(NULL),
        revision(NULL), checksum(NULL) {}
};

/// @cond INTERNAL
class UserLibraryManifest
{
public:
    UserLibraryManifest() {}

    void registerObject(const std::string obj_name)
    { objNames.push_back(obj_name); }

    std::list<std::string> objNames;
};
/// @endcond

/**
 * Representation of the resources user code can ask Vertica for
 */
struct VResources
{
    VResources()
    : scratchMemory(0)
    , nFileHandles(0)
    { }

    /** Amount of RAM in bytes used by User defined function */
    vint scratchMemory;
    /** Number of file handles / sockets required */
    int nFileHandles;
};


/** 
 * @brief Representation of a String in Vertica. All character data is
 *        internally encoded as UTF-8 characters and is not NULL terminated.
 */
class VString
{
private:
    VString();

public:
    /// @cond INTERNAL
    VString(EE::StringValue *sv) : sv(sv) {}
    EE::StringValue *sv; // Opaque pointer to internal representation. Do not modify
    /// @endcond

    /**
     * @brief Returns the length of this VString
     * 
     * @return the length of the string, in bytes. Does not include any extra
     * space for null characters.
     */
    vsize length() const { return lenSV(sv); }

    /**
     * @brief Indicates if this VString contains the SQL NULL value
     * 
     * @return true if this string contains the SQL NULL value, false otherwise
     */
    bool isNull() const { return isNullSV(sv); }

    /**
     * @brief Sets this VString to the SQL NULL value.
     */
    void setNull() { setNullSV(sv); }

    /**
     * @brief Provides a read-only pointer to this VString's internal data.
     * 
     * @return the read only character data for this string, as a pointer. 
     *
     * @note The returned string is \b not null terminated
     */
    const char *data() const { return cvalueSV(sv); }

    /**
     * @brief Provides a copy of this VString's data as a std::string
     * 
     * @return a std::string copy of the data in this VString
     */
    std::string str() const {
        if (isNull() || length() == 0) return std::string("", 0);
        else                           return std::string(data(), length());
    }

    /**
     * @brief Provides a writeable pointer to this VString's internal data.
     * 
     * @return the writeable character data for this string, as a pointer. 
     *
     * @note The returned string is \b not null terminated
     */
    char *data() { return strSV(sv); }

    /**
     * @brief Copy character data from C string to the VString's internal buffer.
     * 
     * Data is copied from s into this VString. It is the caller's
     * responsibility to not provide a value of 'len' that is larger than the
     * maximum size of this VString
     *
     * @param s   character input data
     * 
     * @param len  length in bytes, \b not including the terminating null character
     */
    void copy(const char *s, vsize len) { setSV(sv, OutDA, s, len); }

    /**
     * @brief Copy character data from null terminated C string to the VString's internal buffer.
     * 
     * @param s   null-terminated character input data
     */
    void copy(const char *s)            { copy(s, strlen(s)); }

    /**
     * @brief Copy character data from std::string
     * 
     * @param s   null-terminated character input data
     */
    void copy(const std::string &s)      { copy(s.c_str(), s.size()); }

    /**
     * @brief Copy data from another VString.
     *
     * @param from  The source VString
     *
     **/
    void copy(const VString *from) { copySV(sv, OutDA, from->sv); }

    /**
     * @brief Copy data from another VString.
     *
     * @param from  The source VString
     *
     **/
    void copy(const VString &from) { copy(&from); }

    /**
     * @brief Indicates whether some other VString is equal to this one
     *
     * @return -1 if both are SQL NULL, 0 if not equal, 1 if equal
     * so you can easily consider two NULL values to be equal to each other, or not
     */
    int equal(const VString *other) const { return eqSV(sv, OutDA, other->sv, OutDA); }

    /**
     * @brief Compares this VString to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other (just like memcmp)
     * 
     * @note SQL NULL compares greater than anything else; two SQL NULLs are considered equal
     */
    int compare(const VString *other) const { return cmpSV(sv, OutDA, other->sv, OutDA); }
};

/** 
 * @brief Representation of NUMERIC, fixed point data types in Vertica.
 */
class VNumeric
{
private:
    VNumeric();

public:
    /// @cond INTERNAL
    uint64 *words;
    int nwds;
    int typmod;

    /**
     * Convert a human-readable string (stored in a char*) to a VNumeric
     *
     * @param str A C-style string to parse.  Must be null-terminated.
     * @param type Type and scale for the target VNumeric
     * @param target VNumeric to write the value into
     *
     * @return true if the value could be stored into the specified VNumeric/scale; false if not
     */
    static bool charToNumeric(char *str, const VerticaType &type, VNumeric &target);


    VNumeric(uint64 *words, int32 t)
        : words(words), typmod(t)
    {
        nwds = Basics::getNumericLength(typmod) >> 3;
    }
    /// @endcond

    /**
     * @brief Create a VNumeric with the provided storage location, precision and scale
     *
     * @note It is the callers responsibility to allocate enough memory for the \c words parameter
     */
    VNumeric(uint64 *words, int32 precision, int32 scale)
        : words(words)
    {
        nwds = Basics::getNumericLength(TYPMODFROMPRECSCALE(precision, scale)) >> 3;
        typmod = TYPMODFROMPRECSCALE(precision, scale);
    }

    /**
     * @brief Indicates if this VNumeric is the SQL NULL value
     */
    bool isNull() const 
    { return Basics::BigInt::isNull(words, nwds); }

    /**
     * @brief Sets this VNumeric to the SQL NULL value.
     */
    void setNull() 
    { Basics::BigInt::setNull(words, nwds); }

    /**
     * @brief Indicates if this VNumeric is zero
     */
    bool isZero() const 
    { return Basics::BigInt::isZero(words, nwds); }

    /**
     * @brief Sets this VNumeric to zero
     */
    void setZero() 
    { Basics::BigInt::setZero(words, nwds); }

    /**
     * @brief Copy data from another VNumeric.
     *
     * @param from  The source VNumeric
     *
     **/
    void copy(const VNumeric *from) 
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            Basics::BigInt::copy(words, from->words, nwds); 
        else
            Basics::BigInt::castNumeric(words, nwds, typmod, from->words, from->nwds,from->typmod);
    }

    /**
     * @brief Copy data from a floating-point number
     *
     * @param value  The source floating-point number
     * @param round  Truncates if false; otherwise the numeric result is rounded
     *
     * @return false if conversion failed (precision lost or overflow, etc); true otherwise
     **/
    bool copy(ifloat value, bool round = true)
    {
        const Basics::FiveToScale *fivetoscale = &Basics::fiveToScales[SCALEFROMTYPMOD(typmod)];
        return Basics::BigInt::setFromFloat(words, nwds, *fivetoscale, value, round);
    }

    /**
     * @brief Convert the VNumeric value into floating-point
     *
     * @return the value in 80-bit floating-point
     **/
    ifloat toFloat() const
    {
        ifloat tenthtoscale = powl(10.0L, -SCALEFROMTYPMOD(typmod));
        return Basics::BigInt::numericToFloat(words, nwds, tenthtoscale);
    }

    /**
     * @brief Indicates if this VNumeric is negative
     */
    bool isNeg() const 
    { return Basics::BigInt::isNeg(words, nwds); }

    /**
     * @brief Inverts the sign of this VNumeric (equivalent to multiplying
     * this VNumeric by -1)
     */
    void invertSign()
    { Basics::BigInt::invertSign(words, nwds); }

    /**
     * @brief Indicates whether some other VNumeric is equal to this one
     *
     */
    bool equal(const VNumeric *from) const 
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::isEqualNN(words, from->words, nwds); 
        else
        {
            int tempnwds = (nwds >= from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::isEqualNN(words, tempwords, nwds); 
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::isEqualNN(from->words, tempwords, from->nwds); 
            }
        }
    }
    
    /**
     * @brief Compares this (signed) VNumeric to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other 
     * 
     * @note SQL NULL compares less than anything else; two SQL NULLs are considered equal
     */
    int compare(const VNumeric *from) const 
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::compareNN(words, from->words, nwds); 
        else
        {
            int tempnwds = (nwds > from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::compareNN(words, tempwords, nwds); 
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::compareNN(tempwords, from->words, from->nwds); 
            }
        }
    }

    /**
     * @brief Compares this (unsigned) VNumeric to another
     *
     * @return -1 if this < other, 0 if equal, 1 if this > other 
     * 
     * @note SQL NULL compares less than anything else; two SQL NULLs are considered equal
     */
    int compareUnsigned(const VNumeric *from) const 
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            return Basics::BigInt::ucompareNN(words, from->words, nwds); 
        else
        {
            int tempnwds = (nwds > from->nwds)? nwds : from->nwds;
            uint64 tempwords[tempnwds];
            if (Basics::getNumericPrecision(typmod) >= Basics::getNumericPrecision(from->typmod))
            {
                Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
                return Basics::BigInt::ucompareNN(words, tempwords, nwds); 
            }
            else
            {
                Basics::BigInt::castNumeric(tempwords, from->nwds, from->typmod, words, nwds, typmod);
                return Basics::BigInt::ucompareNN(tempwords, from->words, from->nwds); 
            }
        }
    }

    /* @brief Adds two VNumerics and stores the result in this VNumeric
    */
    void add(const VNumeric *a, const VNumeric *b) 
    {
        if (Basics::isSimilarNumericTypmod(a->typmod, this->typmod) && Basics::isSimilarNumericTypmod(b->typmod, this->typmod))
            Basics::BigInt::addNN(words, a->words, b->words, nwds);
        else
        {
            uint64 tempa[nwds];
            uint64 tempb[nwds];
            Basics::BigInt::castNumeric(tempa, nwds, typmod, a->words, a->nwds, a->typmod);
            Basics::BigInt::castNumeric(tempb, nwds, typmod, b->words, b->nwds, b->typmod);
            Basics::BigInt::addNN(words, tempa, tempb, nwds); 
        }
    }

    /// @brief Adds another VNumeric to this VNumeric
    void accumulate(const VNumeric *from) 
    {
        if (Basics::isSimilarNumericTypmod(from->typmod, this->typmod))
            Basics::BigInt::accumulateNN(words, from->words, nwds); 
        else
        {
            uint64 tempwords[nwds];
            Basics::BigInt::castNumeric(tempwords, nwds, typmod, from->words, from->nwds,from->typmod);
            Basics::BigInt::accumulateNN(words, tempwords, nwds); 
        }
    }

    /* @brief Subtract one VNumeric from another VNumeric and stores the result in this VNumeric
    */
    void sub(const VNumeric *a, const VNumeric *b) 
    {
        if (Basics::isSimilarNumericTypmod(a->typmod, this->typmod) && Basics::isSimilarNumericTypmod(b->typmod, this->typmod))
            Basics::BigInt::subNN(words, a->words, b->words, nwds);
        else
        {
            uint64 tempa[nwds];
            uint64 tempb[nwds];
            Basics::BigInt::castNumeric(tempa, nwds, typmod, a->words, a->nwds, a->typmod);
            Basics::BigInt::castNumeric(tempb, nwds, typmod, b->words, b->nwds, b->typmod);
            Basics::BigInt::subNN(words, tempa, tempb, nwds); 
        }
    }

    /* @brief Increment by 1, no NULL checking
    */
    void incr() 
    { Basics::BigInt::incrementNN(words, nwds); }

    /* @brief Shift a VNumeric to the left (<<) by the given number of bits.
     * The input must be positive and non-NULL.
     */
    void shiftLeft(unsigned bitsToShift) 
    { Basics::BigInt::shiftLeftNN(words, nwds, bitsToShift); }

    /* @brief Shift a VNumeric to the right (>>) by the given number of bits.
     * The input must be positive and non-NULL.
     */
    void shiftRight(unsigned bitsToShift) 
    { Basics::BigInt::shiftRightNN(words, nwds, bitsToShift); }

    /* @brief Multiply two VNumerics and stores the result in this VNumeric
     * Caller need to make sure there is enough scale in this VNumeric to hold the result
     */
    void mul(const VNumeric *a, const VNumeric *b)
    {
        Basics::BigInt::numericMultiply(a->words, a->nwds,
                                        b->words, b->nwds,
                                        words, nwds);
    }

    /* @brief Divide a VNumeric by another VNumeric and stores the result in this VNumeric
     * Caller need to make sure there is enough scale in this VNumeric to hold the result
     */
    void div(const VNumeric *a, const VNumeric *b)
    {
        Basics::BigInt::numericDivide(a->words, a->nwds, a->typmod,
                                      b->words, b->nwds, b->typmod,
                                      words, nwds, typmod);
    }

    /*
     * @brief Convert a VNumeric to a string. The character array should be allocated by the user. 
     * @param[outBuf] A character array where the output is stored.
     * @param[olem]   Length of the character array. As a guideline, the array should be at least 64 bytes long.
     */
    void toString(char* outBuf, int olen) const {
        int scale = SCALEFROMTYPMOD(typmod);
        Basics::BigInt::binaryToDecScale(words, nwds, 
                outBuf, olen, scale);
    }
};

/**
 * @brief Represents types of data that are passed into and returned back from
 *        user's code
 */
class VerticaType
{
public:
    /// @cond INTERNAL
    VerticaType(Oid oid, int32 typmod)
    : typeOid(oid)
    , typeMod(typmod)
    { }

    BaseDataOID getTypeOid() const { return typeOid; }
    int32 getTypeMod() const { return typeMod; }

    void setTypeOid(BaseDataOID oid) { typeOid = oid; }
    void setTypeMod(int typmod) { typeMod = typmod; }
    /// @endcond

    /// @brief Return true for VARCHAR/CHAR/VARBINARY/BINARY/LONG VARCHAR/LONG VARBINARY data types
    bool isStringType() const 
    {
        return isStringOid(getUnderlyingType());
    }

    bool isStringOid(Oid typeOid) const
    {
        return(typeOid == VarcharOID   || typeOid == CharOID ||
               typeOid == VarbinaryOID || typeOid == BinaryOID ||
               typeOid == LongVarcharOID || typeOid == LongVarbinaryOID);
    }

    /// @brief For VARCHAR/CHAR/VARBINARY/BINARY data types, returns the length of the string
    int32 getStringLength() const
    {
        VIAssert(isStringType());
        return typeMod - VARHDRSZ;
    }

    /// @brief For NUMERIC data types, returns the precision
    int32 getNumericPrecision() const
    { return Basics::getNumericPrecision(typeMod); }

    int32 getNumericWordCount() const
    { return Basics::getNumericWordCount(getNumericPrecision()); }

    /// @brief For NUMERIC data types, returns the scale
    int32 getNumericScale() const
    { return Basics::getNumericScale(typeMod); }

    /// @brief For NUMERIC data types, returns the number of fractional digits (i.e., digits right of the decimal point)
    int32 getNumericFractional() const
    { return getNumericScale(); }

    /// @brief For NUMERIC data types, returns the number of integral digits (i.e., digits left of the decimal point)
    int32 getNumericIntegral() const
    { return getNumericPrecision() - getNumericScale(); }

    /// @brief For NUMERIC data types, sets the precision
    void setNumericPrecision(int32 precision)
    { typeMod = makeNumericTypeMod(precision, getNumericScale()); }

    /// @brief For NUMERIC data types, sets the scale
    void setNumericScale(int32 scale)
    { typeMod = makeNumericTypeMod(getNumericPrecision(), scale); }

    /// @brief For NUMERIC data types, returns the number of bytes required to store an element. Calling this with a non-numeric data type can cause a crash
    int32 getNumericLength() const 
    { return Basics::getNumericLength(typeMod); }

    /*
     * Interval helpers
     */
    /// @brief For INTERVAL data types, returns the precision
    int32 getIntervalPrecision() const
    { return INTERVAL_PRECISION(typeMod); }
    
    /// @brief For INTERVAL data types, returns the range
    int32 getIntervalRange() const
    { return INTERVAL_RANGE(typeMod); }

    /// @brief For INTERVAL data types, sets the precision
    void setIntervalPrecision(int32 precision)
    { typeMod = makeIntervalTypeMod(precision, getIntervalRange()); }

    /// @brief For INTERVAL data types, sets the range
    void setIntervalRange(int32 range)
    { typeMod = makeIntervalTypeMod(getIntervalPrecision(), range); }

    /*
     * Timestamp helpers
     */
    /// @brief For TIMESTAMP data types, returns the precision
    int32 getTimestampPrecision() const
    { return typeMod; }
    
    /// @brief For TIMESTAMP data types, sets the precision
    void setTimestampPrecision(int32 precision)
    { typeMod = precision; }

    /*
     * Time helpers
     */
    /// @brief For TIMESTAMP data types, returns the precision
    int32 getTimePrecision() const
    { return typeMod; }
    
    /// @brief For TIMESTAMP data types, sets the precision
    void setTimePrecision(int32 precision)
    { typeMod = precision; }

    /*
     * typmod makers
     */
    /// @cond INTERNAL
    static int32 makeStringTypeMod(int32 len)
    { return len + VARHDRSZ; }
    
    static int32 makeNumericTypeMod(int32 precision, int32 scale)
    { return TYPMODFROMPRECSCALE(precision, scale); }
    
    static int32 makeIntervalYMTypeMod(int32 range)
    { return INTERVAL_TYPMOD(0, range); }
    
    static int32 makeIntervalTypeMod(int32 precision, int32 range)
    { return INTERVAL_TYPMOD(precision, range); }
    
    static int32 makeTimestampTypeMod(int32 precision)
    { return precision; }

    static int32 makeTimeTypeMod(int32 precision)
    { return precision; }
    /// @endcond

    /// @brief Return human readable type string.
    std::string getPrettyPrintStr() const {
        std::ostringstream oss;
        oss << getTypeStr();
        switch (typeOid) {
        case DateOID:
        case TimeOID:
        case TimestampOID:
        case TimestampTzOID:
        case TimeTzOID:
            oss << "(" << typeMod << ")";
            break;
        case IntervalOID:
        case IntervalYMOID:
            char buf[64];
            describeIntervalTypeMod(buf, typeMod);
            oss << buf;
            break;
        case NumericOID:
            oss << "(" << getNumericPrecision() << ","
                << getNumericScale() << ")";
            break;
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
                oss << "(" << getStringLength() << ")";
                break;
        default:
            break;
        };
        return oss.str();
    }

    const char *getTypeStr() const {
        switch (typeOid) {
#define T(x) case x##OID: return #x
            T(Int8);
            T(Time);
            T(TimeTz);
            T(Date);
            T(Float8);
            T(Bool);
            T(Interval);
            T(IntervalYM);
            T(Timestamp);
            T(TimestampTz);
            T(Numeric);
            T(Char);
            T(Varchar);
            T(Binary);
            T(Varbinary);
            T(LongVarchar);
            T(LongVarbinary);
            default: return "UNKNOWN";
#undef T
        }
    }

    /**
     * @brief Returns the maximum size, in bytes, of a data element of this type.
     */
    int32 getMaxSize() const {
        switch(getUnderlyingType()) 
        {
          case Int8OID:
          case TimeOID:
          case TimeTzOID:
          case DateOID:        return sizeof(vint);
          case Float8OID:      return sizeof(vfloat);
          case BoolOID:        return sizeof(vbool);
          case IntervalOID:
          case IntervalYMOID:
          case TimestampOID:
          case TimestampTzOID: return sizeof(vint); 
          case NumericOID:     return getNumericLength();
          case CharOID:
          case VarcharOID:
          case LongVarcharOID:
          case BinaryOID:
          case VarbinaryOID:
          case LongVarbinaryOID: return getStringLength() + sizeof(struct EE::StringValue);
          default:             return 0;
        }
    }
    
    inline bool operator==(const VerticaType &rhs) const
    { return typeOid == rhs.typeOid && typeMod == rhs.typeMod; }

    inline bool operator!=(const VerticaType &rhs) const
    { return !(*this == rhs); }

    /**
     * @brief Returns true if this type is INTEGER, false otherwise.
     */
    bool isInt() const { return (typeOid == Int8OID); }

    /**
     * @brief Returns true if this type is FLOAT, false otherwise.
     */
    bool isFloat() const { return (typeOid == Float8OID); }

    /**
     * @brief Returns true if this type is BOOLEAN, false otherwise.
     */
    bool isBool() const { return (typeOid == BoolOID); }

    /**
     * @brief Returns true if this type is DATE, false otherwise.
     */
    bool isDate() const { return (typeOid == DateOID); }

    /**
     * @brief Returns true if this type is INTERVAL, false otherwise.
     */
    bool isInterval() const { return (typeOid == IntervalOID); }

    /**
     * @brief Returns true if this type is INTERVAL YEAR TO MONTH, false otherwise.
     */
    bool isIntervalYM() const { return (typeOid == IntervalYMOID); }

    /**
     * @brief Returns true if this type is TIMESTAMP, false otherwise.
     */
    bool isTimestamp() const { return (typeOid == TimestampOID); }

    /**
     * @brief Returns true if this type is TIMESTAMP WITH TIMEZONE, false otherwise.
     */
    bool isTimestampTz() const { return (typeOid == TimestampTzOID); }

    /**
     * @brief Returns true if this type is TIME, false otherwise.
     */
    bool isTime() const { return (typeOid == TimeOID); }

    /**
     * @brief Returns true if this type is TIME WITH TIMEZONE, false otherwise.
     */
    bool isTimeTz() const { return (typeOid == TimeTzOID); }

    /**
     * @brief Returns true if this type is NUMERIC, false otherwise.
     */
    bool isNumeric() const { return (typeOid == NumericOID); }

    /**
     * @brief Returns true if this type is CHAR, false otherwise.
     */
    bool isChar() const { return (typeOid == CharOID); }

    /**
     * @brief Returns true if this type is VARCHAR, false otherwise.
     */
    bool isVarchar() const { return (typeOid == VarcharOID); }

    /**
     * @brief Returns true if this type is LONG VARCHAR, false otherwise.
     */
    bool isLongVarchar() const { return (typeOid == LongVarcharOID); }

    /**
     * @brief Returns true if this type is BINARY, false otherwise.
     */
    bool isBinary() const { return (typeOid == BinaryOID); }

    /**
     * @brief Returns true if this type is VARBINARY, false otherwise.
     */
    bool isVarbinary() const { return (typeOid == VarbinaryOID); }

    /**
     * @brief Returns true if this type is LONG VARCHAR, false otherwise.
     */
    bool isLongVarbinary() const { return (typeOid == LongVarbinaryOID); }


    /**
     * @return If this is a built in type, returns the typeOid. Otherwise, if a
     * user defined type returns the base type oid on which the user type is
     * based.
     *
     * Note: This function is designed so that the common case (aka a built in,
     * non user defined type) is fast -- calling isUDType() is a heavyweight
     * operation. -- See VER-24673 for an example of when it matters.
     */
    Oid getUnderlyingType() const
    {
        switch (typeOid) {
          case VUnspecOID:       /* fallthrough */
          case VTuple:           /* fallthrough */
          case VPosOID:          /* fallthrough */
          case RecordOID:        /* fallthrough */
          case UnknownOID:       /* fallthrough */
          case BoolOID:          /* fallthrough */
          case Int8OID:          /* fallthrough */
          case Float8OID:        /* fallthrough */
          case CharOID:          /* fallthrough */
          case VarcharOID:       /* fallthrough */
          case DateOID:          /* fallthrough */
          case TimeOID:          /* fallthrough */
          case TimestampOID:     /* fallthrough */
          case TimestampTzOID:   /* fallthrough */
          case IntervalOID:      /* fallthrough */
          case IntervalYMOID:    /* fallthrough */
          case TimeTzOID:        /* fallthrough */
          case NumericOID:       /* fallthrough */
          case VarbinaryOID:     /* fallthrough */
          case RLETuple:         /* fallthrough */
          case BinaryOID:        /* fallthrough */
          case LongVarbinaryOID: /* fallthrough */
          case LongVarcharOID:   /* fallthrough */
            return typeOid;
          default: 
              Oid aliasedTypeOid = getUDTypeUnderlyingOid(typeOid);
              if (aliasedTypeOid == VUnspecOID)
                  VIAssert("Unknown data type" == 0);
              return aliasedTypeOid;
        }
    }

private:
    Oid typeOid;
    int32 typeMod;
};

/**
 * @brief Represents (unsized) types of the columns used as input/output of a
 * User Defined Function/Transform Function.
 *
 * This class is used only for generating the function or transform function
 * prototype, where the sizes and/or precisions of the data types are not
 * known.
 */
class ColumnTypes
{
public:

    /// @brief Adds a column of type INTEGER
    void addInt() { colTypes.push_back(Int8OID); }

    /// @brief Adds a column of type FLOAT
    void addFloat() { colTypes.push_back(Float8OID); }

    /// @brief Adds a column of type BOOLEAN
    void addBool() { colTypes.push_back(BoolOID); }

    /// @brief Adds a column of type DATE
    void addDate() { colTypes.push_back(DateOID); }

    /// @brief Adds a column of type INTERVAL/INTERVAL DAY TO SECOND
    void addInterval() { colTypes.push_back(IntervalOID); }

    /// @brief Adds a column of type INTERVAL YEAR TO MONTH
    void addIntervalYM() { colTypes.push_back(IntervalYMOID); }

    /// @brief Adds a column of type TIMESTAMP
    void addTimestamp() { colTypes.push_back(TimestampOID); }

    /// @brief Adds a column of type TIMESTAMP WITH TIMEZONE
    void addTimestampTz() { colTypes.push_back(TimestampTzOID); }

    /// @brief Adds a column of type TIME
    void addTime() { colTypes.push_back(TimeOID); }

    /// @brief Adds a column of type TIME WITH TIMEZONE
    void addTimeTz() { colTypes.push_back(TimeTzOID); }
    /// @brief Adds a column of type NUMERIC
    void addNumeric() { colTypes.push_back(NumericOID); }

    /// @brief Adds a column of type CHAR
    void addChar() { colTypes.push_back(CharOID); }

    /// @brief Adds a column of type VARCHAR
    void addVarchar() { colTypes.push_back(VarcharOID); }

    /// @brief Adds a column of type BINARY
    void addBinary() { colTypes.push_back(BinaryOID); }

    /// @brief Adds a column of type VARBINARY
    void addVarbinary() { colTypes.push_back(VarbinaryOID); }

    /// @brief Adds a column of type VARBINARY
    void addLongVarchar() { colTypes.push_back(LongVarcharOID); }

    /// @brief Adds a column of type VARBINARY
    void addLongVarbinary() { colTypes.push_back(LongVarbinaryOID); }

    /// @brief Indicates that function can take any number and type of arguments
    void addAny() { varArgs = true; }

    void addUserDefinedType(const char *typeName) {
        colTypes.push_back(getUDTypeOid(typeName));
    }

    /// @cond INTERNAL
    size_t getColumnCount() const { return colTypes.size(); }

    // Returns the type of the field at the specified index. 
    BaseDataOID getColumnType(size_t idx) const { return colTypes.at(idx); }

    // Update the type of the specified column
    void setColumnType(size_t idx, BaseDataOID type) {
        colTypes.at(idx) = type; // bounds check
    }
    
    // Default constructor - no varArgs (yet)
    ColumnTypes() : varArgs(false) {}

    bool isVarArgs() const { return varArgs; }

    std::vector<BaseDataOID> colTypes;
    /// @endcond

private:
    bool varArgs;
};

/** 
 * @brief Represents the partition by and order by column information for
 * each phase in a multi-phase transform function
 *
 */
struct PartitionOrderColumnInfo
{
    PartitionOrderColumnInfo() : last_partition_col(-1), last_order_col(-1) {}

    int last_partition_col;
    int last_order_col;
};

/** 
 * @brief Represents types and information to determine the size of the columns
 *        as input/output of a User Defined Function/Transform
 *
 * This class is used to exchange size and precision information between
 * Vertica and the user defined function/transform function. Vertica provides
 * the user code with size/precision information about the particular data
 * types that it has been called with, and expects the user code to provide
 * size/precision information about what it will return.
 */
class SizedColumnTypes
{
public:
    /// @cond INTERNAL
    /* Add a new element to the tuple at the end */
    void addArg(const VerticaType &dt, const std::string &fieldName = "")
    { argTypes.push_back(Field(dt, fieldName)); }

    /* Add a new element to the tuple at the end */
    void addArg(Oid typeOid, int32 typeMod, const std::string &fieldName = "") 
    { addArg(VerticaType(typeOid, typeMod), fieldName); }

    /* Add a new partition element to the tuple at the end of the Pby columns */
    void addPartitionColumn(Oid typeOid, int32 typeMod, const std::string &fieldName = "")
    { addPartitionColumn(VerticaType(typeOid, typeMod), fieldName); }

    /* Add a new order element to the tuple at the end of the Oby columns */
    void addOrderColumn(Oid typeOid, int32 typeMod, const std::string &fieldName = "")
    { addOrderColumn(VerticaType(typeOid, typeMod), fieldName); }
    /// @endcond

    /// @brief Adds a partition column of the specified type
    ///        (only relevant to multiphase UDTs.)
    void addPartitionColumn(const VerticaType &dt, const std::string &fieldName = "")
    {
        int newLastPbyIdx = partitionOrderInfo.last_partition_col + 1;
        std::vector<Field>::iterator it = argTypes.begin();

        for (int i = 0; i < newLastPbyIdx && it < argTypes.end(); i++)
            it++;
        
        argTypes.insert(it, Field(dt, fieldName));
        partitionOrderInfo.last_partition_col = newLastPbyIdx;
        partitionOrderInfo.last_order_col++;
    }

    /// @brief Adds an order column of the specified type.
    ///        (only relevant to multiphase UDTs.)
    void addOrderColumn(const VerticaType &dt, const std::string &fieldName = "")
    {
        int newLastObyIdx = partitionOrderInfo.last_order_col + 1;
        std::vector<Field>::iterator it = argTypes.begin();

        for (int i = 0; i < newLastObyIdx && it < argTypes.end(); i++)
            it++;
        
        argTypes.insert(it, Field(dt, fieldName));
        partitionOrderInfo.last_order_col = newLastObyIdx;
    }

    /// @brief Adds a column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addInt(const std::string &fieldName = "")
    { addArg(Int8OID, -1, fieldName); }

    /// @brief Adds a column of type FLOAT
    ///
    /// @param fieldName The name for the output column.    
    void addFloat(const std::string &fieldName = "")
    { addArg(Float8OID, -1, fieldName); }

    /// @brief Adds a column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.    
    void addBool(const std::string &fieldName = "")
    { addArg(BoolOID, -1, fieldName); }

    /// @brief Adds a column of type DATE
    ///
    /// @param fieldName The name for the output column.    
    void addDate(const std::string &fieldName = "")
    { addArg(DateOID, -1, fieldName); }

    /// @brief Adds a column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.    
    void addInterval(int32 precision, int32 range, const std::string &fieldName = "")
    { addArg(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    /// @brief Adds a column of type INTERVAL YEAR TO MONTH
    ///
    ///
    /// @param range The range for the interval.
    ///    
    /// @param fieldName The name for the output column. 
    void addIntervalYM(int32 range, const std::string &fieldName = "")
    { addArg(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    /// @brief Adds a column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.     
    void addTimestamp(int32 precision, const std::string &fieldName = "")
    { addArg(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a column of type TIMESTAMP WITH TIMEZONE 
    ///
    /// @param precision The precision for the timestamp.
    ///    
    /// @param fieldName The name for the output column.        
    void addTimestampTz(int32 precision, const std::string &fieldName = "")
    { addArg(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTime(int32 precision, const std::string &fieldName = "")
    { addArg(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTimeTz(int32 precision, const std::string &fieldName = "")
    { addArg(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a column of type NUMERIC
    ///    
    /// @param precision The precision for the numeric value.
    /// 
    /// @param scale The scale for the numeric value.
    /// 
    /// @param fieldName The name for the output column.         
    void addNumeric(int32 precision, int32 scale, const std::string &fieldName = "")
    { addArg(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    /// @brief Adds a column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addChar(int32 len, const std::string &fieldName = "")
    { addArg(CharOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds a column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarchar(int32 len, const std::string &fieldName = "")
    { addArg(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a column of type LONG VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarchar(int32 len, const std::string &fieldName = "")
    { addArg(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addBinary(int32 len, const std::string &fieldName = "")
    { addArg(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds a column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarbinary(int32 len, const std::string &fieldName = "")
    { addArg(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a column of type LONG VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addLongVarbinary(int32 len, const std::string &fieldName = "")
    { addArg(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a column of a user-defined type
    ///
    /// @param typeName the name of the type
    /// @param len the length of the type field, in bytes
    /// @param fieldName the name of the field
    ///
    /// @param fieldName The name for the output column.     
    void addUserDefinedType(const char *typeName, int32 len, const std::string &fieldName = "")
    {
        // neil todo - what if the aliased type is not a string type
        addArg(getUDTypeOid(typeName), VerticaType::makeStringTypeMod(len), fieldName);
    }

    /// @brief Adds a partition column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addIntPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(Int8OID, -1, fieldName); }

    /// @brief Adds a partition column of type FLOAT
    ///
    /// @param fieldName The name for the output column.    
    void addFloatPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(Float8OID, -1, fieldName); }

    /// @brief Adds a partition column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.    
    void addBoolPartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(BoolOID, -1, fieldName); }

    /// @brief Adds a partition column of type DATE
    ///
    /// @param fieldName The name for the output column.    
    void addDatePartitionColumn(const std::string &fieldName = "")
    { addPartitionColumn(DateOID, -1, fieldName); }

    /// @brief Adds a partition column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.    
    void addIntervalPartitionColumn(int32 precision, int32 range, const std::string &fieldName = "")
    { addPartitionColumn(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    /// @brief Adds a partition column of type INTERVAL YEAR TO MONTH
    ///
    /// @param range The range for the interval.
    ///    
    /// @param fieldName The name for the output column. 
    void addIntervalYMPartitionColumn(int32 range, const std::string &fieldName = "")
    { addPartitionColumn(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    /// @brief Adds a partition column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.     
    void addTimestampPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIMESTAMP WITH TIMEZONE 
    ///
    /// @param precision The precision for the timestamp.
    ///    
    /// @param fieldName The name for the output column.        
    void addTimestampTzPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTimePartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTimeTzPartitionColumn(int32 precision, const std::string &fieldName = "")
    { addPartitionColumn(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds a partition column of type NUMERIC
    ///    
    /// @param precision The precision for the numeric value.
    /// 
    /// @param scale The scale for the numeric value.
    /// 
    /// @param fieldName The name for the output column.         
    void addNumericPartitionColumn(int32 precision, int32 scale, const std::string &fieldName = "")
    { addPartitionColumn(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    /// @brief Adds a partition column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addCharPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(CharOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds a partition column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarcharPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a partition column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarcharPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a partition column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addBinaryPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds a partition column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarbinaryPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds a partition column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarbinaryPartitionColumn(int32 len, const std::string &fieldName = "")
    { addPartitionColumn(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds an order column of type INTEGER
    ///
    /// @param fieldName The name for the output column.
    void addIntOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(Int8OID, -1, fieldName); }

    /// @brief Adds an order column of type FLOAT
    ///
    /// @param fieldName The name for the output column.    
    void addFloatOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(Float8OID, -1, fieldName); }

    /// @brief Adds an order column of type BOOLEAN
    ///
    /// @param fieldName The name for the output column.    
    void addBoolOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(BoolOID, -1, fieldName); }

    /// @brief Adds an order column of type DATE
    ///
    /// @param fieldName The name for the output column.    
    void addDateOrderColumn(const std::string &fieldName = "")
    { addOrderColumn(DateOID, -1, fieldName); }

    /// @brief Adds an order column of type INTERVAL/INTERVAL DAY TO SECOND
    ///
    /// @param precision The precision for the interval.
    ///
    /// @param range The range for the interval.
    ///
    /// @param fieldName The name for the output column.    
    void addIntervalOrderColumn(int32 precision, int32 range, const std::string &fieldName = "")
    { addOrderColumn(IntervalOID, VerticaType::makeIntervalTypeMod(precision, range), fieldName); }

    /// @brief Adds an order column of type INTERVAL YEAR TO MONTH
    ///
    ///
    /// @param range The range for the interval.
    ///    
    /// @param fieldName The name for the output column. 
    void addIntervalYMOrderColumn(int32 range, const std::string &fieldName = "")
    { addOrderColumn(IntervalYMOID, VerticaType::makeIntervalYMTypeMod(range), fieldName); }

    /// @brief Adds an order column of type TIMESTAMP
    ///
    /// @param precision The precision for the timestamp.
    ///
    /// @param fieldName The name for the output column.     
    void addTimestampOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimestampOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIMESTAMP WITH TIMEZONE 
    ///
    /// @param precision The precision for the timestamp.
    ///    
    /// @param fieldName The name for the output column.        
    void addTimestampTzOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimestampTzOID, VerticaType::makeTimestampTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIME
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTimeOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimeOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type TIME WITH TIMEZONE
    ///
    /// @param precision The precision for the time.
    ///
    /// @param fieldName The name for the output column.     
    void addTimeTzOrderColumn(int32 precision, const std::string &fieldName = "")
    { addOrderColumn(TimeTzOID, VerticaType::makeTimeTypeMod(precision), fieldName); }

    /// @brief Adds an order column of type NUMERIC
    ///    
    /// @param precision The precision for the numeric value.
    /// 
    /// @param scale The scale for the numeric value.
    /// 
    /// @param fieldName The name for the output column.         
    void addNumericOrderColumn(int32 precision, int32 scale, const std::string &fieldName = "")
    { addOrderColumn(NumericOID, VerticaType::makeNumericTypeMod(precision, scale), fieldName); }

    /// @brief Adds an order column of type CHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addCharOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(CharOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds an order column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarcharOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(VarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds an order column of type VARCHAR
    ///
    /// @param len The length of the string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarcharOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(LongVarcharOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds an order column of type BINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addBinaryOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(BinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }
    
    /// @brief Adds an order column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.     
    void addVarbinaryOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(VarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Adds an order column of type VARBINARY
    ///
    /// @param len The length of the binary string.
    ///
    /// @param fieldName The name for the output column.
    void addLongVarbinaryOrderColumn(int32 len, const std::string &fieldName = "")
    { addOrderColumn(LongVarbinaryOID, VerticaType::makeStringTypeMod(len), fieldName); }

    /// @brief Returns the number of columns
    size_t getColumnCount() const { return argTypes.size(); }

    /// @brief Returns the type of the column at the specified index. 
    /// @param idx The index of the column 
    /// @return a VerticaType object describing the column's data type.
    const VerticaType &getColumnType(size_t idx) const {
        return argTypes.at(idx)._type; // bounds check
    }

    /// @brief Returns the type of the column at the specified index. 
    /// @param idx The index of the column 
    VerticaType &getColumnType(size_t idx) {
        return argTypes.at(idx)._type; // bounds check
    }

    /*
     * Update the type of the specified column
     */
    /// @cond INTERNAL
    void setColumnType(size_t idx, const VerticaType &t, 
                       const std::string &fieldName = "") {
        argTypes.at(idx) = Field(t, fieldName); // bounds check
    }
    /// @endcond

    /// @brief Returns the name of the column at the specified index
    /// @param idx The index of the column 
    const std::string &getColumnName(size_t idx) const {
        return argTypes.at(idx)._name; // bounds check
    }

    /// @brief Indicates whether the column at the specified index is a PARTITION BY column
    /// @param idx The index of the column 
    bool isPartitionByColumn(int idx) const { 
        return idx <= partitionOrderInfo.last_partition_col; 
    }

    /// @brief Indicates whether the column at the specified index is an ORDER BY column
    /// @param idx The index of the column 
    bool isOrderByColumn(int idx) const { 
        return idx > partitionOrderInfo.last_partition_col && 
            idx <= partitionOrderInfo.last_order_col; 
    }

    /// @brief Sets the PARTITION BY and ORDER BY column indexes
    /// @param partition_idx Index of the last partition-by column
    /// @param order_idx Index of the last order-by column
    void setPartitionOrderColumnIdx(int partition_idx, int order_idx)
    {
        partitionOrderInfo.last_partition_col = partition_idx;
        partitionOrderInfo.last_order_col = order_idx;
    }

    /// @brief Sets the PARTITION BY and ORDER BY column indexes from another SizedColumnTypes object
    /// @param other The SizedColumnTypes object to set the indexes from
    void setPartitionOrderColumnIdx(const SizedColumnTypes &other) {
        partitionOrderInfo = other.partitionOrderInfo;
    }

    /// @brief Gets the last PARTITION BY column index
    int getLastPartitionColumnIdx() const {
        return partitionOrderInfo.last_partition_col;
    }

    /// @brief Gets the last ORDER BY column index
    int getLastOrderColumnIdx() const {
        return partitionOrderInfo.last_order_col;
    }

    /// @brief Retrieves indexes of PARTITION BY columns in the OVER() clause.
    ///        Indexes in cols can be used in conjunction with other functions, 
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getPartitionByColumns(std::vector<size_t>& cols) const {
        cols.clear();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (lastPbyIdx > -1) {
            // Append to cols the sequence: [0, 1, ..., lastPbyIdx]
            for (size_t i = 0; i <= (size_t) lastPbyIdx; i++)
                cols.push_back(i);
        }
    }

    /// @brief Retrieves indexes of ORDER BY columns in the OVER() clause.
    ///        Indexes in cols can be used in conjunction with other functions, 
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getOrderByColumns(std::vector<size_t>& cols) const {
        cols.clear();
        int lastObyIdx = getLastOrderColumnIdx();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (lastObyIdx > lastPbyIdx) {
            // Append to cols the sequence: [i, i+1, ..., lastObyIdx]
            for (size_t i = lastPbyIdx + 1; i <= (size_t) lastObyIdx; i++)
                cols.push_back(i);
        }
    }

    /// @brief Retrieves indexes of argument columns.
    ///        Indexes in cols can be used in conjunction with other functions,
    ///        e.g. getColumnType(size_t) and getColumnName(size_t).
    /// @param cols A vector to store the retrieved column indexes.
    void getArgumentColumns(std::vector<size_t>& cols) const {
        cols.clear();
        size_t nOverCl = getOverClauseCount();
        size_t nCols = getColumnCount();

        // Arguments follow the Over() clause columns.
        for (size_t i = nOverCl; i < nCols; i++) {
            cols.push_back(i);
        }
    }

private:
    struct Field 
    {
        Field(const VerticaType &t, const std::string name) : 
            _type(t), _name(name) {}

        VerticaType      _type;
        std::string      _name; 
    };

    std::vector<Field> argTypes;     // list of argument type
    PartitionOrderColumnInfo partitionOrderInfo;

    /// @cond INTERNAL
    /// Returns the number of columns in the OVER() clause.
    size_t getOverClauseCount() const {
        int nPbyOby = getLastOrderColumnIdx();
        int lastPbyIdx = getLastPartitionColumnIdx();

        if (nPbyOby < lastPbyIdx)
            nPbyOby = lastPbyIdx;

        return ++nPbyOby;
    }
    /// @endcond
};

/**
 * VTAllocator is a pool based allocator that is provided to simplify memory
 * management for UDF implementors. 
 */
class VTAllocator
{
public:
    virtual ~VTAllocator() {}

    /**
     * Allocate size_t bytes of memory on a pool. This memory is guaranteed to
     * persist beyond the destroy call but might have been destroyed when the
     * dtor is run.
     */
    virtual void *alloc(size_t size) = 0;

    // construct an object/objects in Vertica managed memory area
    // not exposed for this release
//    virtual void *constr(size_t obj_size, void (*destr)(void *), size_t array_size = 1) = 0;
};

/**
 * MetaData interface for Vertica User Defined extensions
 */
class UDXFactory 
{
public:
    virtual ~UDXFactory() {}

    /**
     * Provides the argument and return types of the UDX
     */
    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) = 0;

    /**
     * Function to tell Vertica what the return types (and length/precision if
     * necessary) of this UDX are.  
     *
     * For CHAR/VARCHAR types, specify the max length,
     *
     * For NUMERIC types, specify the precision and scale. 
     *
     * For Time/Timestamp types (with or without time zone),
     * specify the precision, -1 means unspecified/don't care
     *
     * For IntervalYM/IntervalDS types, specify the precision and range
     *
     * For all other types, no length/precision specification needed
     *
     * @param argTypes Provides the data types of arguments that
     *                       this UDT was called with. This may be used 
     *                       to modify the return types accordingly.
     *
     * @param returnType User code must fill in the names and data 
     *                       types returned by the UDT.
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType) = 0;

    /**
     * Function to tell Vertica the name and types of parameters that this
     * function uses. Vertica will use this to warn function callers that certain
     * parameters they provide are not affecting anything, or that certain
     * parameters that are not being set are reverting to default values.
     */
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {}

    /** The type of UDX instance this factory produces */
    enum UDXType
    {
        /// @cond INTERNAL
        INVALID_UDX, // Invalid type (used for initialization)
        /// @endcond
        FUNCTION, // User Defined Scalar Function
        TRANSFORM, // User Defined Transform Function
        ANALYTIC, // User Defined Analytic Function
        MULTI_TRANSFORM, // Multi-phase transform function
        AGGREGATE, // User Defined Aggregate Function
        LOAD_SOURCE, // Data source for Load operations
        LOAD_FILTER, // Intermediate processing for raw Load stream data
        LOAD_PARSER, // User Defined Parser
        TYPE,
    };

    /**
     * @return the type of UDX Object instance this factory returns.
     *
     * @note User subclasses should use the appropriate subclass of
     * UDXFactory and not override this method on their own.
     */
    virtual UDXType getUDXFactoryType() = 0;

    /**
     * Set the resource required for each instance of the UDX Object subclass
     * 
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * @param res a VResources object used to tell Vertica what resources are needed by the UDX
     */
    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res) {}


protected:
    // set by Vertica to mirror the object name created in DDL
    // used for providing sensible error message
    std::string sqlName;

    // used by Vertica internally
    Oid libOid;

    friend class UdfSupport;
};

/**
 * Base class for Vertica User Defined extensions, the object themselves
 */
class UDXObject
{
public:
    /**
     * Destructors MAY NOT throw errors / exceptions. Exceptions thrown during
     * the destructor will be ignored.
     */
    virtual ~UDXObject() {}

    /**
     * Perform per instance initialization. This function may throw errors.
     */
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {}

    /**
     * Perform per instance destruction.  This function may throw errors
     */
    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {}

};

class UDXObjectCancelable : public UDXObject
{
public:
    UDXObjectCancelable() : canceled(false) {}

    /**
     * This function is invoked from a different thread when the execution is canceled
     * This baseclass cancel should be called in any override.
     */
    virtual void cancel(ServerInterface &srvInterface)
    {
        canceled = true;
    }

    /**
     * Returns true if execution was canceled.
     */
    bool isCanceled() { return canceled; }

protected:
    volatile bool canceled;
};

/**
 * Enums to allow programmatic specification of volatility and strictness
 */
enum volatility {
    DEFAULT_VOLATILITY,
    VOLATILE,
    IMMUTABLE,
    STABLE
};

enum strictness {
    DEFAULT_STRICTNESS,
    CALLED_ON_NULL_INPUT,
    RETURN_NULL_ON_NULL_INPUT,
    STRICT
};

/**
 * Interface for User Defined Scalar Function, the actual code to process
 * a block of data.
 */
class ScalarFunction : public UDXObject
{
public:
    /**
     * Invoke a user defined function on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param arg_reader input rows
     *
     * @param res_writer output location
     *
     * @note @li This methods may be invoked by different threads at different
     * times, and by a different thread than the constructor.
     *
     * @li The order in which the function sees rows is not guaranteed.
     *
     * @li C++ exceptions may NOT be thrown out of this method. Use the vertica
     * specific vt_throw_exception() function or vt_report_error() macro instead
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &arg_reader,
                              BlockWriter &res_writer) = 0;
protected:
    // used by Vertica internally
    enum InterfaceType
    {
        FunctionT,
        IndexListFunctionT,
    };
    virtual InterfaceType getInterfaceType() { return FunctionT; }

    friend class UdfSupport;
};

/**
 * MetaData interface for Vertica User Defined Scalar Functions.
 *
 * A ScalarFunctionFactory is responsible for providing type and type modifier
 * information.
 */
class ScalarFunctionFactory : public UDXFactory
{
public:
    ScalarFunctionFactory() : vol(DEFAULT_VOLATILITY), strict(DEFAULT_STRICTNESS) {}
    virtual ~ScalarFunctionFactory() {}
 
    /**
     * For scalar functions, this function needs to be overridden only if the
     * return type needs length/precision specification.
     * 
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * 
     * @param argTypes The data type of the return value defined by processBlock()
     *
     * @param returnType The size of the data returned by processBlock()
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        ColumnTypes p_argTypes;
        ColumnTypes p_returnType;
        getPrototype(srvInterface, p_argTypes, p_returnType);
        if (p_returnType.getColumnCount() != 1) {
            ereport(ERROR,
                    (errmsg("User Defined Scalar Function can only have 1 return column, but %zu is provided",
                            p_returnType.getColumnCount()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        BaseDataOID typeOid = p_returnType.getColumnType(0);
        switch (p_returnType.getColumnType(0))
        {
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case NumericOID:
        case IntervalOID:
        case IntervalYMOID:
        case TimestampOID:
        case TimestampTzOID:
        case TimeOID:
        case TimeTzOID:
            ereport(ERROR,
                    (errmsg("The data type requires length/precision specification"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            break;
        default:
            returnType.addArg(typeOid, -1); // use default typmod
        }
    }

    /**
     * @return an ScalarFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * 
     * @note More than one object may be instantiated per query.
     *
     * 
     * 
     */
    virtual ScalarFunction *createScalarFunction(ServerInterface &srvInterface) = 0;

    /**
     * Strictness and Volatility settings that the UDSF programmer can set
     * Defaults are VOLATILE and CALLED_ON_NULL_INPUT
     */
    volatility vol;
    strictness strict;
    
protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return FUNCTION; }

};

/** 
 * Interface for User Defined Transform, the actual code to process
 * a partition of data coming in as a stream
 */
class TransformFunction : public UDXObjectCancelable
{
public:
    /**  
     * Invoke a user defined transform on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param input_reader input rows
     *
     * @param output_writer output location
     */
    virtual void processPartition(ServerInterface &srvInterface, 
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer) = 0;
};

/**
 *Interface to provide User Defined Transform compile time information
 */
class TransformFunctionFactory : public UDXFactory
{
public:
    virtual ~TransformFunctionFactory() {}

    /** Called when Vertica needs a new TransformFunction object to process a UDTF
     * function call. 
     *  
     * @return an TransformFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * 
     * @note More than one object may be instantiated per query.
     *
     * 
     * 
     */
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return TRANSFORM; }
};

/** 
 * Interface for User Defined Analytic, the actual code to process
 * a partition of data coming in as a stream
 *
 */
class AnalyticFunction : public UDXObjectCancelable
{
public:
    /**  
     * Invoke a user defined analytic on a set of rows. As the name suggests, a
     * batch of rows are passed in for every invocation to amortize performance.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @param input_reader input rows
     *
     * @param output_writer output location
     */
    virtual void processPartition(ServerInterface &srvInterface, 
                                  AnalyticPartitionReader &input_reader,
                                  AnalyticPartitionWriter &output_writer) = 0;
};

/** 
 * Interface to provide User Defined Analytic compile time information
 */
class AnalyticFunctionFactory : public UDXFactory
{
public:
    virtual ~AnalyticFunctionFactory() {}

    /** Called when Vertica needs a new AnalyticFunction object to process a function call. 
     *  
     * @return an AnalyticFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * 
     * @note More than one object may be instantiated per query.
     *
     * 
     * 
     */
    virtual AnalyticFunction *createAnalyticFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return ANALYTIC; }
};

/** 
 * Interface for User Defined Aggregate, the actual code to process
 * a block of data coming in from the stream
 */
class AggregateFunction : public UDXObject
{
public:

    /**
     * Called by the server to perform aggregation on multiple blocks of data
     *
     * @note User should not explicity implement this function. It is implemented by
     * calling the InlineAggregate() macro. User should follow the convention of implementing
     * void aggregate(ServerInterface &srvInterface,
     *                 BlockReader &arg_reader,
     *                 IntermediateAggs &aggs)
     * along with initAggregate, combine, and terminate. For references on what a fully
     * implemented Aggregate Function looks like, check the examples in the example folder.
     *
     * which the inlined aggregateArrs implemention will invoke
     */
    virtual void aggregateArrs(ServerInterface &srvInterface, void **dstTuples,
                               int doff, const void *arr, int stride, const void *rcounts,
                               int rcstride, int count, IntermediateAggs &intAggs,
                               std::vector<int> &intOffsets, BlockReader &arg_reader) = 0;
    

    /**
     * Called by the server to set the starting values of the Intermediate aggregates.
     *
     * @note This can be called multiple times on multiple machines, so starting values
     * should be idempotent.
     */
    virtual void initAggregate(ServerInterface &srvInterface, 
                               IntermediateAggs &aggs) = 0;

    /**
     * Called when intermediate aggregates need to be combined with each other
     */
    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs_output, 
                         MultipleIntermediateAggs &aggs_other) = 0;

    /**
     * Called by the server to get the output to the aggregate function
     */
    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &res_writer, 
                           IntermediateAggs &aggs) = 0;

    /**
     * Helper function for aggregateArrs.
     *
     * @note User should not call this function.
     */
    static void updateCols(BlockReader &arg_reader, char *arg, int count,
                           IntermediateAggs &intAggs, char *aggPtr, std::vector<int> &intOffsets);
};

/** 
 * Interface to provide User Defined Aggregate compile time information
 */
class AggregateFunctionFactory : public UDXFactory
{
public:
    virtual ~AggregateFunctionFactory() {}

    /**
     * Returns the intermediate types used for this aggregate. Called by the
     * server to set the types of the Intermediate aggregates.
     */
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes, 
                                      SizedColumnTypes &intermediateTypeMetaData) = 0;

    /** Called when Vertica needs a new AggregateFunction object to process a function call. 
     *  
     * @return an AggregateFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     * 
     * @note More than one object may be instantiated per query.
     */
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return AGGREGATE; }
};

/**
 * @brief: Represents an in-memory block of tuples
 */
class VerticaBlock
{
public:
    VerticaBlock(size_t ncols, int rowcount)
    : ncols(ncols), count(rowcount),
      index(0)
    {
        cols.reserve(ncols);
        colstrides.reserve(ncols);
        vnWrappers.reserve(ncols);
        svWrappers.reserve(ncols);
    }

    /**
     * @return the number of columns held by this block.
     */
    size_t getNumCols() const { return ncols; }

    /**
     * @return the number of rows held by this block.
     */
    int getNumRows() const { return count; }

    /**
     * @return information about the types and numbers of arguments
     */
    const SizedColumnTypes &getTypeMetaData() const { return typeMetaData; }

    /**
     * @return information about the types and numbers of arguments
     */
    SizedColumnTypes &getTypeMetaData() { return typeMetaData; }

    /** 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * Example:
     *
     * @code
     * const vint *a = arg_reader->getColPtr<vint>(0);
     * @endcode
     */
    template <class T> 
    const T *getColPtr(size_t idx) { return reinterpret_cast<const T *>(cols.at(idx)); }

    template <class T> 
    T *getColPtrForWrite(size_t idx) { return reinterpret_cast<T *>(cols.at(idx)); }

    /** 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * Example:
     * const vint a = arg_reader->getColRef<vint>(0);
     */
    template <class T>
    const T &getColRef(size_t idx) { return *getColPtr<T>(idx); }

    template <class T>
    T &getColRefForWrite(size_t idx) { return *getColPtrForWrite<T>(idx); }

protected:
    /**
     * Add the location for reading a particular argument.
     *
     * @param arg The base location to find data.
     * 
     * @param colstride The stride between data instances.
     * 
     * @param dt The type of input.
     *
     * @param fieldname the name of the field
     */
    void addCol(char *arg, int colstride, const VerticaType &dt, const std::string fieldName = "")
    {
        cols.push_back(arg);
        colstrides.push_back(colstride);
        int32 typmod = (dt.getTypeOid() == NumericOID
                        ? dt.getTypeMod()
                        : VerticaType::makeNumericTypeMod(2,1));
        vnWrappers.push_back(VNumeric(0, typmod));
        svWrappers.push_back(VString(0));
        typeMetaData.addArg(dt, fieldName);
        if (ncols < cols.size()) ncols = cols.size();
    }

    void addCol(const char *arg, int colstride, const VerticaType &dt, const std::string fieldName = "")
    {
        addCol(const_cast<char*>(arg), colstride, dt, fieldName);
    }

    // Private debugging: ensure generated strings confirm to declared sizes
    void validateStringColumn(size_t idx, const VString &s, const VerticaType &t)
    {
        if (!s.isNull() && s.length() > size_t(t.getStringLength()))
        {
            ereport(ERROR,
                    (errmsg("Returned string value '[%s]' with length [%zu] "
                            "is greater than declared field length of [%zu] "
                            "of field [%s] at output column index [%zu]",
                            s.str().c_str(),
                            size_t(s.length()),
                            size_t(t.getStringLength()),
                            typeMetaData.getColumnName(0).c_str(),
                                idx),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
    }

    std::vector<char *> cols;          // list of pointers that holds the cols
    std::vector<int> colstrides;       // list of stride values for each col
    size_t ncols;                      // number of cols
    int count;                         // for input: total number of tuples in block, for output: total capacity
    int index;                         // for input: number of rows already read, for output: number of rows already written

    SizedColumnTypes typeMetaData;     // what types, etc.

    std::vector<VString> svWrappers;   // pre-create the VString objects to avoid per-call creation
    std::vector<VNumeric> vnWrappers;  // pre-create the VNumeric objects to avoid per-call creation

    friend class VerticaBlockSerializer;
    friend class EE::UserDefinedProcess;
    friend class EE::UserDefinedAnalytic;
    friend class EE::UserDefinedTransform;
    friend class EE::UserDefinedAggregate;
    friend void AggregateFunction::updateCols(BlockReader &arg_reader, char *arg, int count,
                                              IntermediateAggs &intAggs, char *aggPtr,
                                              std::vector<int> &intOffsets);
    friend void deserializeVerticaParametersBlock(VerticaBlock* block, const std::string &str);
};


/**
 * @brief Iterator interface for reading rows in a Vertica block.
 *
 * This class provides the input to the ScalarFunction.processBlock() 
 * function. You extract values from the input row using data type
 * specific functions to extract each column value. You can also
 * determine the number of columns and their data types, if your
 * processBlock function does not have hard-coded input expectations.
 */
class BlockReader : public VerticaBlock
{
public:
    BlockReader(size_t narg, int rowcount) :
        VerticaBlock(narg, rowcount) {}

    /** 
     * @brief Get a pointer to an INTEGER value from the input row.
     * 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param idx The column number to retrieve from the input row.
     * 
     * Example:
     * 
     * @code
     * const vint *a = arg_reader->getIntPtr(0);
     * @endcode
     */
    const vint *getIntPtr(size_t idx)                { return getColPtr<vint>(idx); }
    /// @brief Get a pointer to a FLOAT value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    const vfloat *getFloatPtr(size_t idx)            { return getColPtr<vfloat>(idx); }
    /// @brief Get a pointer to a BOOLEAN value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    const vbool *getBoolPtr(size_t idx)              { return getColPtr<vbool>(idx); }
    /// @brief Get a pointer to a DATE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    const DateADT *getDatePtr(size_t idx)            { return getColPtr<DateADT>(idx); }
    /// @brief Get a pointer to an INTERVAL value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.    
    const Interval *getIntervalPtr(size_t idx)       { return getColPtr<Interval>(idx); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.    
    const IntervalYM *getIntervalYMPtr(size_t idx)   { return getColPtr<IntervalYM>(idx); }
    /// @brief Get a pointer to a TIMESTAMP value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.    
    const Timestamp *getTimestampPtr(size_t idx)     { return getColPtr<Timestamp>(idx); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .    
    const TimestampTz *getTimestampTzPtr(size_t idx) { return getColPtr<TimestampTz>(idx); }
    /// @brief Get a pointer to a TIME value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME.    
    const TimeADT *getTimePtr(size_t idx)            { return getColPtr<TimeADT>(idx); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.    
    const TimeTzADT *getTimeTzPtr(size_t idx)        { return getColPtr<TimeTzADT>(idx); }
    /// @brief Get a pointer to a VNumeric value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.    
    const VNumeric *getNumericPtr(size_t idx)    
    { 
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint>(idx));
        vnWrappers[idx].words = const_cast<uint64*>(words);
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    const VString *getStringPtr(size_t idx)    
    { 
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint>(idx));
        svWrappers[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappers[idx];
    }

    /** 
     * @brief Get a reference to an INTEGER value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTEGER.
     *
     * Example:
     *
     * @code 
     * const vint a = arg_reader->getIntRef(0);
     * @endcode
     */
    const vint        &getIntRef(size_t idx)         { return *getIntPtr(idx); }
    /** 
     * @brief Get a reference to a FLOAT value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return A reference to the idx'th argument, cast as an FLOAT.
     */    
    const vfloat      &getFloatRef(size_t idx)       { return *getFloatPtr(idx); }
    /** 
     * @brief Get a reference to a BOOLEAN value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an BOOLEAN.
     */    
    const vbool       &getBoolRef(size_t idx)        { return *getBoolPtr(idx); }
    /** 
     * @brief Get a reference to a DATE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an DATE.
     */    
    const DateADT     &getDateRef(size_t idx)        { return *getDatePtr(idx); }
    /** 
     * @brief Get a reference to an INTERVAL value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTERVAL.
     */     
    const Interval    &getIntervalRef(size_t idx)    { return *getIntervalPtr(idx); }
    /** 
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTERVAL YEAR TO MONTH.
     */     
    const IntervalYM  &getIntervalYMRef(size_t idx)  { return *getIntervalYMPtr(idx); }
    /** 
     * @brief Get a reference to a TIMESTAMP value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIMESTAMP.
     */     
    const Timestamp   &getTimestampRef(size_t idx)   { return *getTimestampPtr(idx); }
    /** 
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIMESTAMP WITH TIMEZONE.
     */     
    const TimestampTz &getTimestampTzRef(size_t idx) { return *getTimestampTzPtr(idx); }
    /** 
     * @brief Get a reference to a TIME value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIME.
     */     
    const TimeADT     &getTimeRef(size_t idx)       { return *getTimePtr(idx); }
    /** 
     * @brief Get a reference to a TIME WITH TIMEZONE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIME WITH TIMEZONE.
     */     
    const TimeTzADT   &getTimeTzRef(size_t idx)     { return *getTimeTzPtr(idx); }
    /** 
     * @brief Get a reference to a VNumeric value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an VNumeric.
     */     
    const VNumeric    &getNumericRef(size_t idx)     { return *getNumericPtr(idx); }
    /** 
     * @brief Get a reference to an VString value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an VString.
     */     
    const VString     &getStringRef(size_t idx)      { return *getStringPtr(idx); }

    /**
     * Advance to the next record.
     *
     * @return true if there are more rows to read, false otherwise.
     */
    bool next() 
    {
        if (++index >= count) return false;

        for (size_t i = 0; i < ncols; ++i)
            cols[i] += colstrides[i];
        return true;
    }

    /**
     * @brief  Check if the idx'th argument is null 

     * @param col The column number in the row to check for null
     * @return true is the col value is null false otherwise
     */
    bool isNull(int col)
    {
        int nCols=getNumCols(); 
        VIAssert(col < nCols );
        const SizedColumnTypes &inTypes = getTypeMetaData(); 
        const VerticaType &t = inTypes.getColumnType(col); 
        BaseDataOID oid = t.getTypeOid(); 

        switch (oid) 
        { 
        case BoolOID:{ 
            vbool a = getBoolRef(col); 
            return(a == vbool_null); 
        } 
            break; 
        case Float8OID:	{ 
            vfloat a = getFloatRef(col); 
            return ( vfloatIsNull (a) ) ; 
        } 
            break; 
        case BinaryOID: 
        case VarbinaryOID: 
        case LongVarbinaryOID:
        case CharOID: 
        case VarcharOID: 
        case LongVarcharOID:
        { 
            VString a = getStringRef(col); 
            return ( a.isNull() ); 
        } 
        break; 
        case NumericOID:{ 
            VNumeric a = getNumericRef(col); 
            return ( a.isNull() ) ; 
        } 
            break; 
        case Int8OID:{ 
            vint a = getIntRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case IntervalOID:{ 
            Interval a = getIntervalRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case TimestampOID:{ 
            Timestamp a = getTimestampRef(col); 
            return (a == vint_null) ; 
        } 
            break; 
        case TimestampTzOID:{ 
            TimestampTz a = getTimestampTzRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case TimeOID:{ 
            TimeADT a = getTimeRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case TimeTzOID:{ 
            TimeTzADT a = getTimeTzRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case DateOID:{ 
            DateADT a = getDateRef(col); 
            return (a == vint_null); 
        } 
            break; 
        case IntervalYMOID:{ 
            IntervalYM a = getIntervalYMRef(col); 
            return (a == vint_null); 
        } 
            break; 
        default: 
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        } 
        //should never get here, prevents compiler warning 
        return false; 
    }
    
    friend class EE::VEval;
    friend class VerticaRInterface;
};

/**
 * @brief Iterator interface for writing rows to a Vertica block
 * 
 * This class provides the output rows that ScalarFunction.processBlock()
 * writes to. 
 */
class BlockWriter : public VerticaBlock
{
public:
    BlockWriter(char *outArr, int stride, int rowcount, const VerticaType &dt)
        : VerticaBlock(1, rowcount)
    {
        addCol(outArr, stride, dt);
    }

    /**
     * Setter methods
     */
    
    /// @brief Adds an INTEGER value to the output row.
    /// @param r The INTEGER value to insert into the output row.
    void setInt(vint r)                { getColRefForWrite<vint>(0) = r; }
    /// @brief Adds a FLOAT value to the output row.
    /// @param r The FLOAT value to insert into the output row.    
    void setFloat(vfloat r)            { getColRefForWrite<vfloat>(0) = r; }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.    
    void setBool(vbool r)              { getColRefForWrite<vbool>(0) = r; }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.    
    void setDate(DateADT r)            { getColRefForWrite<DateADT>(0) = r; }
    /// @brief Adds an INTERVAL value to the output row.
    /// @param r The INTERVAL value to insert into the output row.     
    void setInterval(Interval r)       { getColRefForWrite<Interval>(0) = r; }
    /// @brief Adds an INTERVAL YEAR TO MONTH value to the output row.
    /// @param r The INTERVAL YEAR TO MONTH value to insert into the output row. 
    void setIntervalYM(IntervalYM r)   { getColRefForWrite<IntervalYM>(0) = r; }
    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.     
    void setTimestamp(Timestamp r)     { getColRefForWrite<Timestamp>(0) = r; }
    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row. 
    void setTimestampTz(TimestampTz r) { getColRefForWrite<TimestampTz>(0) = r; }
    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.     
    void setTime(TimeADT r)            { getColRefForWrite<TimeADT>(0) = r; }
    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row. 
    void setTimeTz(TimeTzADT r)        { getColRefForWrite<TimeTzADT>(0) = r; }

    /// @brief Allocate a new VNumeric object to use as output.
    ///
    /// @return A new VNumeric object to hold output. This object 
    ///         automatically added to the output row.
    VNumeric &getNumericRef()          
    { 
        uint64 *words = getColPtrForWrite<uint64>(0);
        vnWrappers[0].words = words;
        return vnWrappers[0];
    }
    /// @brief Allocates a new VString object to use as output.
    ///
    /// @return A new VString object to hold output. This object 
    ///         automatically added to the output row.
    VString &getStringRef()            
    { 
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(0);
        svWrappers[0].sv = sv;
        return svWrappers[0];
    }

    void *getVoidPtr() { return reinterpret_cast<void *>(cols.at(0)); }

    /// @brief Complete writing this row of output and move to the next row.
    ///
    /// 
    void next()
    {
        const VerticaType &t = typeMetaData.getColumnType(0);
        if (t.isStringType()) {
            validateStringColumn(0, getStringRef(), t);
        }

        if (++index >= count)
            return;

        cols[0] += colstrides[0];
    }

private:

    friend class EE::VEval;
};


/**
 * @brief: A wrapper around a single intermediate aggregate value
 *
 */
class IntermediateAggs : public VerticaBlock
{
public:
    IntermediateAggs() : VerticaBlock(0, 0) {}
    IntermediateAggs(size_t ninter) : VerticaBlock(ninter, 1) {} 

    /** 
     * @brief Get a pointer to an INTEGER value from the input row.
     * 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param idx The column number to retrieve from the input row.
     * 
     * Example:
     * 
     * @code
     *  vint *a = arg_reader->getIntPtr(0);
     * @endcode
     */
    vint *getIntPtr(size_t idx)                { return getColPtrForWrite<vint>(idx); }
    /// @brief Get a pointer to a FLOAT value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    vfloat *getFloatPtr(size_t idx)            { return getColPtrForWrite<vfloat>(idx); }
    /// @brief Get a pointer to a BOOLEAN value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    vbool *getBoolPtr(size_t idx)              { return getColPtrForWrite<vbool>(idx); }
    /// @brief Get a pointer to a DATE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    DateADT *getDatePtr(size_t idx)            { return getColPtrForWrite<DateADT>(idx); }
    /// @brief Get a pointer to an INTERVAL value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.    
    Interval *getIntervalPtr(size_t idx)       { return getColPtrForWrite<Interval>(idx); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.    
    IntervalYM *getIntervalYMPtr(size_t idx)   { return getColPtrForWrite<IntervalYM>(idx); }
    /// @brief Get a pointer to a TIMESTAMP value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.    
    Timestamp *getTimestampPtr(size_t idx)     { return getColPtrForWrite<Timestamp>(idx); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .    
    TimestampTz *getTimestampTzPtr(size_t idx) { return getColPtrForWrite<TimestampTz>(idx); }
    /// @brief Get a pointer to a TIME value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME.    
    TimeADT *getTimePtr(size_t idx)            { return getColPtrForWrite<TimeADT>(idx); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the input row.
    ///
    /// @param idx The column number in the input row to retrieve.
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.    
    TimeTzADT *getTimeTzPtr(size_t idx)        { return getColPtrForWrite<TimeTzADT>(idx); }
    /// @brief Get a pointer to a VNumeric value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.    
    VNumeric *getNumericPtr(size_t idx)    
    { 
        uint64 *words = reinterpret_cast< uint64 *>(getColPtrForWrite<vint>(idx));
        vnWrappers[idx].words = words;
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the input row.
    ///
    /// @param idx The column number to retrieve from the input row.
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    VString *getStringPtr(size_t idx)    
    { 
        EE::StringValue *sv = reinterpret_cast<EE::StringValue *>(getColPtrForWrite<vint>(idx));
        svWrappers[idx].sv = sv;
        return &svWrappers[idx];
    }

    /** 
     * @brief Get a reference to an INTEGER value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTEGER.
     *
     * Example:
     *
     * @code 
     *  vint a = arg_reader->getIntRef(0);
     * @endcode
     */
    vint        &getIntRef(size_t idx)         { return *getIntPtr(idx); }
    /** 
     * @brief Get a reference to a FLOAT value from the input row.
     
     * @param idx The column number to retrieve from the input row.
     * 
     * @return A reference to the idx'th argument, cast as an FLOAT.
     */    
    vfloat      &getFloatRef(size_t idx)       { return *getFloatPtr(idx); }
    /** 
     * @brief Get a reference to a BOOLEAN value from the input row.
     
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an BOOLEAN.
     */    
    vbool       &getBoolRef(size_t idx)        { return *getBoolPtr(idx); }
    /** 
     * @brief Get a reference to a DATE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an DATE.
     */    
    DateADT     &getDateRef(size_t idx)        { return *getDatePtr(idx); }
    /** 
     * @brief Get a reference to an INTERVAL value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTERVAL.
     */     
    Interval    &getIntervalRef(size_t idx)    { return *getIntervalPtr(idx); }
    /** 
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an INTERVAL YEAR TO MONTH.
     */     
    IntervalYM  &getIntervalYMRef(size_t idx)  { return *getIntervalYMPtr(idx); }
    /** 
     * @brief Get a reference to a TIMESTAMP value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIMESTAMP.
     */     
    Timestamp   &getTimestampRef(size_t idx)   { return *getTimestampPtr(idx); }
    /** 
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIMESTAMP WITH TIMEZONE.
     */     
    TimestampTz &getTimestampTzRef(size_t idx) { return *getTimestampTzPtr(idx); }
    /** 
     * @brief Get a reference to a TIME value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIME.
     */     
    TimeADT     &getTimeRef(size_t idx)       { return *getTimePtr(idx); }
    /** 
     * @brief Get a reference to a TIME WITH TIMEZONE value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as a TIME WITH TIMEZONE.
     */     
    TimeTzADT   &getTimeTzRef(size_t idx)     { return *getTimeTzPtr(idx); }
    /** 
     * @brief Get a reference to a VNumeric value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an VNumeric.
     */     
    VNumeric    &getNumericRef(size_t idx)     { return *getNumericPtr(idx); }
    /** 
     * @brief Get a reference to an VString value from the input row.
    
     * @param idx The column number to retrieve from the input row.
     * 
     * @return a reference to the idx'th argument, cast as an VString.
     */     
    VString     &getStringRef(size_t idx)      { return *getStringPtr(idx); }

private:
};


/**
 * @brief: A wrapper around multiple intermediate aggregates
 * 
 */
class MultipleIntermediateAggs : public BlockReader
{
public:
    MultipleIntermediateAggs() : 
        BlockReader(0,0) {}

    MultipleIntermediateAggs(size_t narg) :
        BlockReader(narg, 0) {}
};

/**
 * @brief: A wrapper around Parameters that have a name->value correspondence
 */
class ParamReader : public VerticaBlock
{
public:
    ParamReader(size_t nparams) : VerticaBlock(nparams, 1) {}
    ParamReader() : VerticaBlock(0, 1) {}

    /**
     * @brief Function to see if the ParamReader has a value for the parameter
     */
    bool containsParameter(std::string paramName)
    {
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        return paramNameToIndex.find(paramName) != paramNameToIndex.end();
    }

    /**
     * @brief Returns true if there are no parameters
     */
    bool isEmpty() const { return paramNameToIndex.empty(); }

    /**
     * @brief Return all names of parameters stored in this ParamReader
     */
    std::vector<std::string> getParamNames() {
        std::vector<std::string> paramNames;

        for (std::map<std::string, size_t>::const_iterator iter = paramNameToIndex.begin();
                iter != paramNameToIndex.end(); ++iter) {
            paramNames.push_back(iter->first);
        }

        return paramNames;
    }

    /**
     * @brief Return the type of the given parameter
     */
    VerticaType getType(std::string paramName) {
        return getTypeMetaData().getColumnType(getIndex(paramName));
    }

    /**
     * @brief Get a pointer to an INTEGER value from the input row.
     * 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * @param paramName The name of the parameter to retrieve
     * 
     * Example:
     * 
     * @code
     *  vint *a = arg_reader->getIntPtr("max");
     * @endcode
     */
    const vint *getIntPtr(std::string paramName)                { return getColPtr<vint>(getIndex(paramName)); }
    /// @brief Get a pointer to a FLOAT value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a FLOAT.
    const vfloat *getFloatPtr(std::string paramName)            { return getColPtr<vfloat>(getIndex(paramName)); }
    /// @brief Get a pointer to a BOOLEAN value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a BOOLEAN.
    const vbool *getBoolPtr(std::string paramName)              { return getColPtr<vbool>(getIndex(paramName)); }
    /// @brief Get a pointer to a DATE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a DATE.
    const DateADT *getDatePtr(std::string paramName)            { return getColPtr<DateADT>(getIndex(paramName)); }
    /// @brief Get a pointer to an INTERVAL value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as an INTERVAL.    
    const Interval *getIntervalPtr(std::string paramName)       { return getColPtr<Interval>(getIndex(paramName)); }
    /// @brief Get a pointer to a INTERVAL YEAR TO MONTH value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A point to the retrieved value cast as a INTERVAL YEAR TO MONTH.    
    const IntervalYM *getIntervalYMPtr(std::string paramName)   { return getColPtr<IntervalYM>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIMESTAMP value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP.    
    const Timestamp *getTimestampPtr(std::string paramName)     { return getColPtr<Timestamp>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIMESTAMP WITH TIMEZONE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIMESTAMP WITH TIMEZONE .    
    const TimestampTz *getTimestampTzPtr(std::string paramName) { return getColPtr<TimestampTz>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIME value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIME.    
    const TimeADT *getTimePtr(std::string paramName)            { return getColPtr<TimeADT>(getIndex(paramName)); }
    /// @brief Get a pointer to a TIME WITH TIMEZONE value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a TIME WITH TIMEZONE.    
    const TimeTzADT *getTimeTzPtr(std::string paramName)        { return getColPtr<TimeTzADT>(getIndex(paramName)); }
    /// @brief Get a pointer to a VNumeric value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a Numeric.    
    const VNumeric *getNumericPtr(std::string paramName)    
    { 
        size_t idx = getIndex(paramName);
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint>(idx));
        vnWrappers[idx].words = const_cast<uint64*>(words);
        return &vnWrappers[idx];
    }
    /// @brief Get a pointer to a VString value from the input row.
    ///
    /// @param paramName The name of the parameter to retrieve
    ///
    /// @return A pointer to the retrieved value cast as a VString.
    const VString *getStringPtr(std::string paramName)
    { 
        size_t idx = getIndex(paramName);
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint>(idx));
        svWrappers[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappers[idx];
    }

    /** 
     * @brief Get a reference to an INTEGER value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an INTEGER.
     *
     * Example:
     *
     * @code 
     *  vint a = arg_reader->getIntRef("max");
     * @endcode
     */
    const vint        &getIntRef(std::string paramName)         { return *getIntPtr(paramName); }
    /** 
     * @brief Get a reference to a FLOAT value from the input row.
     
     * @param paramName The name of the parameter to retrieve
     * 
     * @return A reference to the parameter value, cast as an FLOAT.
     */    
    const vfloat      &getFloatRef(std::string paramName)       { return *getFloatPtr(paramName); }
    /** 
     * @brief Get a reference to a BOOLEAN value from the input row.
     
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an BOOLEAN.
     */    
    const vbool       &getBoolRef(std::string paramName)        { return *getBoolPtr(paramName); }
    /** 
     * @brief Get a reference to a DATE value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an DATE.
     */    
    const DateADT     &getDateRef(std::string paramName)        { return *getDatePtr(paramName); }
    /** 
     * @brief Get a reference to an INTERVAL value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an INTERVAL.
     */     
    const Interval    &getIntervalRef(std::string paramName)    { return *getIntervalPtr(paramName); }
    /** 
     * @brief Get a reference to an INTERVAL YEAR TO MONTH value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an INTERVAL YEAR TO MONTH.
     */     
    const IntervalYM  &getIntervalYMRef(std::string paramName)  { return *getIntervalYMPtr(paramName); }
    /** 
     * @brief Get a reference to a TIMESTAMP value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as a TIMESTAMP.
     */     
    const Timestamp   &getTimestampRef(std::string paramName)   { return *getTimestampPtr(paramName); }
    /** 
     * @brief Get a reference to a TIMESTAMP WITH TIMEZONE value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as a TIMESTAMP WITH TIMEZONE.
     */     
    const TimestampTz &getTimestampTzRef(std::string paramName) { return *getTimestampTzPtr(paramName); }
    /** 
     * @brief Get a reference to a TIME value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as a TIME.
     */     
    const TimeADT     &getTimeRef(std::string paramName)       { return *getTimePtr(paramName); }
    /** 
     * @brief Get a reference to a TIME WITH TIMEZONE value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as a TIME WITH TIMEZONE.
     */     
    const TimeTzADT   &getTimeTzRef(std::string paramName)     { return *getTimeTzPtr(paramName); }
    /** 
     * @brief Get a reference to a VNumeric value from the input row.
       
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an VNumeric.
     */     
    const VNumeric    &getNumericRef(std::string paramName)     { return *getNumericPtr(paramName); }
    /** 
     * @brief Get a reference to an VString value from the input row.
    
     * @param paramName The name of the parameter to retrieve
     * 
     * @return a reference to the parameter value, cast as an VString.
     */     
    const VString     &getStringRef(std::string paramName)      { return *getStringPtr(paramName); }

    /**
     * Add a parameter to the block and stores it name and corresponding index
     * in the paramNameToIndex map
     */
    void addParameter(std::string paramName, const char *arg, const VerticaType &dt)
    {        
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        addCol(arg, dt.getMaxSize(), dt);
        size_t index = paramNameToIndex.size();
        paramNameToIndex[paramName] = index;
    }

    size_t getIndex(std::string paramName)
    {        
        if (!containsParameter(paramName)) {
            ereport(ERROR, (errmsg("Function attempted to access parameter %s when it was not provided", paramName.c_str())));
        }
        std::transform(paramName.begin(), paramName.end(), paramName.begin(), ::tolower);
        return paramNameToIndex[paramName];
    }

    /** Bookkeepinp to make a parameter and its position in the block */
    std::map<std::string, size_t> paramNameToIndex;

public:

    friend class VerticaBlockSerializer;
};


/**
 * @brief: A wrapper around a map from column to ParamReader.
 */
class PerColumnParamReader
{
private:
    typedef std::map<std::string, ParamReader> StringToParamReader;
    StringToParamReader paramReaderByCol;

public:

    /**
     * @brief Returns true if a ParamReader exists for the given column
     */
    bool containsColumn(std::string columnName) const {
        return paramReaderByCol.count(columnName) > 0;
    }

    /**
     * @brief Gets the names of all columns with column specific arguments
     *
     * @return a vector of column names
     */
    std::vector<std::string> getColumnNames() const {
        std::vector<std::string> ret;
        for (StringToParamReader::const_iterator i=paramReaderByCol.begin();
             i!=paramReaderByCol.end(); ++i)
        {
            ret.push_back(i->first);
        }
        return ret;
    }

    /**
     * @brief Gets the parameters of the given column
     *
     * @param the name of the column of interest
     *
     * @return the parameters of the given column
     */
    ParamReader& getColumnParamReader(const std::string &column) {
        return paramReaderByCol[column];
    }
};


/**
 * PartitionReader provides an iterator-based read interface over all input
 * data in a single partition. Automatically fetches data a block-at-a-time,
 * as needed.
 */
class PartitionReader : public BlockReader
{
public:
    PartitionReader(size_t narg, EE::UserDefinedProcess *udx_object) :
        BlockReader(narg, 0), udx_object(udx_object), dh(0) {}

    virtual ~PartitionReader() {}

    bool next()
    {
        if (!count) return false; // Do nothing if we have no rows to read

        if (++index >= count)
            return readNextBlock();

        for (size_t i = 0; i < ncols; ++i)
            cols[i] += colstrides[i];

        return true;
    }

    /** Reads in the next block of data and positions cursor at the
    * beginning.
    *
    * @return false if there's no more input data
    */
    virtual bool readNextBlock();

protected:
    union {
        EE::UserDefinedProcess *udx_object;
        EE::UserDefinedTransform *udt;
        EE::UserDefinedAnalytic *udan;
    };
    EE::DataHolder *dh;
    vpos pstart; // the offset of the start of this partition within the DataHolder

    virtual void setupColsAndStrides();

    friend class VerticaBlockSerializer;
    friend class EE::UserDefinedProcess;
    friend class EE::UserDefinedTransform;
    friend class EE::UserDefinedAnalytic;
    friend class FullPartition;
};


/**
 * PartitionWriter provides an iterator-based write interface over output data
 * for a single partition. Automatically makes space a block-at-a-time, as
 * needed.
 */
class PartitionWriter : public VerticaBlock
{
public:
    PartitionWriter(size_t narg, EE::UserDefinedProcess *udx_object) :
        VerticaBlock(narg, 0), udx_object(udx_object), pstart(0) {}

    virtual ~PartitionWriter() {}

    /**
     * Setter methods
     */
    void setInt(size_t idx, vint r)                { getColRefForWrite<vint>(idx) = r; }
    void setFloat(size_t idx, vfloat r)            { getColRefForWrite<vfloat>(idx) = r; }
    void setBool(size_t idx, vbool r)              { getColRefForWrite<vbool>(idx) = r; }
    void setDate(size_t idx, DateADT r)            { getColRefForWrite<DateADT>(idx) = r; }
    void setInterval(size_t idx, Interval r)       { getColRefForWrite<Interval>(idx) = r; }
    void setTimestamp(size_t idx, Timestamp r)     { getColRefForWrite<Timestamp>(idx) = r; }
    void setTimestampTz(size_t idx, TimestampTz r) { getColRefForWrite<TimestampTz>(idx) = r; }
    void setTime(size_t idx, TimeADT r)            { getColRefForWrite<TimeADT>(idx) = r; }
    void setTimeTz(size_t idx, TimeTzADT r)        { getColRefForWrite<TimeTzADT>(idx) = r; }

    VNumeric &getNumericRef(size_t idx)
    {
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }
    VString &getStringRef(size_t idx)
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
            svWrappers[idx].setNull(); // Initialize to the null string, for safety
        }
        return svWrappers[idx];
    }
    VString &getStringRefNoClear(size_t idx)  // Don't initialize the string first; unsafe unless we're pointing at a pre-written block
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }
    void validateColumn(size_t idx) {
        // Validate any sized data types
        const VerticaType &t = typeMetaData.getColumnType(idx);
        if (t.isStringType()) {
            validateStringColumn(idx, getStringRefNoClear(idx), t);
        }
    }

    void *getVoidPtr(size_t idx) { return reinterpret_cast<void *>(cols.at(idx)); }

    /**
     * Copies a column from the input reader to the output writer. The data
     * types and sizes of the source and destination columns must match exactly.
     *
     * @param dstIdx The destination column index (in the output writer)
     *
     * @param input_reader The input reader from which to copy a column
     *
     * @param srcIdx The source column index (in the input reader)
     */
    void copyFromInput(size_t dstIdx, PartitionReader &input_reader, size_t srcIdx)
    {
        const char *srcBytes = input_reader.getColPtr<char>(srcIdx);
        char *dstBytes = getColPtrForWrite<char>(dstIdx);

        // Get number of bytes for columns
        const VerticaType &dt = typeMetaData.getColumnType(dstIdx);
        const VerticaType &st = input_reader.getTypeMetaData().getColumnType(srcIdx);
        int dstsize = dt.getMaxSize();
        int srcsize = st.getMaxSize();
        if (dstsize < srcsize)
        {
            ereport(ERROR,
                    (errmsg("The types/sizes of source column (index %zu, length %d) "
                            "and destination column (index %zu, length %d) "
                            "do not match",
                            srcIdx, st.getMaxSize(),
                            dstIdx, dt.getMaxSize()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }

        if (dt.isStringType())
            getStringRef(dstIdx).copy(input_reader.getStringPtr(srcIdx));
        else
            VMEMCPY(dstBytes, srcBytes, dt.getMaxSize());
    }

    bool next()
    {
        if (!count) return false; // do nothing if there's no more rows to write to

        // validate each column
        for (size_t i=0; i < ncols; ++i)
        {
            // check backwards (in case of later overwrites)
            size_t idx = ncols - 1 - i;
            validateColumn(idx);
        }

        if (++index >= count)
        {
            return getWriteableBlock();
        }

        for (size_t i = 0; i < ncols; ++i)
            cols[i] += colstrides[i];

        return true;
    }

    /**
     * @brief  Set the idx'th argument to null 

     * @param idx The column number in the row to set to null
     */
    void setNull(size_t idx)
    {
        VIAssert(idx < ncols );
        const SizedColumnTypes &inTypes = getTypeMetaData(); 
        const VerticaType &t = inTypes.getColumnType(idx); 
        BaseDataOID oid = t.getTypeOid(); 
        switch (oid)
        {
        case Int8OID:
            setInt(idx, vint_null);
            break;
        case Float8OID:
            setFloat(idx, vfloat_null);
            break;
        case BoolOID:
            setBool(idx, vbool_null);
            break;
        case DateOID:
            setDate(idx, vint_null);
            break;
        case IntervalYMOID:
        case IntervalOID:
            setInterval(idx, vint_null);
            break;
        case TimestampOID:
            setTimestamp(idx, vint_null);
            break;
        case TimestampTzOID:
            setTimestampTz(idx, vint_null);
            break;
        case TimeOID:
            setTime(idx, vint_null);
            break;
        case TimeTzOID:
            setTimeTz(idx, vint_null);
            break;
        case BinaryOID:
        case VarbinaryOID:
        case LongVarbinaryOID:
        case CharOID:
        case VarcharOID:
        case LongVarcharOID:
            getStringRef(idx).setNull();
            break;
        case NumericOID:
            getNumericRef(idx).setNull();
            break;
        default:
            ereport(ERROR,
                    (errmsg("Unknown data type"),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        } 
    }

    /**  Gets a writeable block of data and positions cursor at the beginning.
    */
    virtual bool getWriteableBlock();

protected:
    // Opaque pointer to internal server state
    union {
        EE::UserDefinedProcess *udx_object;
        EE::UserDefinedTransform *udt;
        EE::UserDefinedAnalytic *udan;
    };
    int pstart;

    friend class EE::UserDefinedProcess;
    friend class EE::UserDefinedTransform;
    friend class EE::Loader::UserDefinedLoad;
    friend class EE::UserDefinedAnalytic;
};


/**
 * StreamWriter provides an iterator-based write interface over output data
 * for a stream of blocks. Automatically makes space a block-at-a-time, as
 * needed.
 */
class StreamWriter : public PartitionWriter
{
public:
    StreamWriter(size_t narg, EE::Loader::UserDefinedLoad *udl) :
        PartitionWriter(narg, NULL), parent(udl) {}

    /**  Gets a writeable block of data and positions cursor at the beginning.
    */
    virtual bool getWriteableBlock();

    virtual uint64 getTotalRowCount();

protected:
    // Opaque pointer to internal server state
    EE::Loader::UserDefinedLoad *parent;

    friend class EE::Loader::UserDefinedLoad;
};


/**
 * AnalyticPartitionReader provides an iterator-based read interface over all the
 * partition_by keys, order_by keys, and function arguments in a partition.
 */
class AnalyticPartitionReader : public PartitionReader
{
public:
    AnalyticPartitionReader(size_t narg, EE::UserDefinedProcess *udx_object) :
        PartitionReader(narg, udx_object)
        { }

    bool isNewOrderByKey()
    {
        if (!count) return false; // no rows to read - default not a new order by
        return newOrderMarkers[index];
    }

    virtual bool readNextBlock();

protected:
    virtual void setupColsAndStrides();

    std::vector<bool> newOrderMarkers;

    friend class EE::UserDefinedAnalytic;
    friend class EE::UserDefinedProcess;
    friend class AnalyticPartitionReaderHelper;
};


/**
 * AnalyticPartitionWriter provides an iterator-based write interface over all input
 * data in a single partition. It automatically makes space as needed.
 */
class AnalyticPartitionWriter : public PartitionWriter
{
public:
    AnalyticPartitionWriter(size_t narg, EE::UserDefinedProcess *udx_object) :
        PartitionWriter(narg, udx_object) { }

    virtual ~AnalyticPartitionWriter() {}

    /**  Gets a writeable block of data and positions cursor at the beginning.
    */
    virtual bool getWriteableBlock();
};

/**
 * Helper function for inlining aggregates by swapping out the pointers for the
 * next block of input args and that block's corresponding intermediateAggs
 */
inline void AggregateFunction::updateCols(BlockReader &arg_reader, char *arg, int count,
                                          IntermediateAggs &intAggs, char *aggPtr,
                                          std::vector<int> &intOffsets) {
    arg_reader.cols[0] = arg;
    arg_reader.count = count;
    arg_reader.index = 0;

    for (uint i=0; i<intOffsets.size(); ++i)
        intAggs.cols[i] = aggPtr + intOffsets[i];
}


/////////////////////////////// Multi-Phase UDTs ////////////////////////////////
/**
 * Interface to provide compile time information for a single phase of a
 * multi-phase user-defined transform function. Note that even though this
 * class shares some methods with TransformFunctionFactory, it is explicitly
 * not a sub-class (since it is incorrect to implement a getPrototype() method
 * for this class)
 */
class TransformFunctionPhase
{
public:

    TransformFunctionPhase() : prepass(false) {}

    virtual ~TransformFunctionPhase() {}

    /**
     * Function to tell Vertica what the return types (and length/precision if
     * necessary) and partition-by, order-by of this phase are.
     *
     * For CHAR/VARCHAR types, specify the max length,
     *
     * For NUMERIC types, specify the precision and scale.
     *
     * For Time/Timestamp types (with or without time zone),
     * specify the precision, -1 means unspecified/don't care
     *
     * For IntervalYM/IntervalDS types, specify the precision and range
     *
     * For all other types, no length/precision specification needed
     *
     * @param argTypes Provides the data types of arguments that this phase
     *                 was called with, along with partition and order
     *                 information. This may be used to modify the return
     *                 types accordingly.
     *
     * @param returnTypes User code must fill in the names and data types
     *                    returned by this phase, along with the partition-by
     *                    and order-by column information (if any).
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes) = 0;

    /**
     * Called when Vertica needs a new TransformFunction object to process
     * this phase of a multi-phase UDT.
     *
     * @return a TransformFunction object which implements the UDx API described
     * by this metadata.
     *
     * @param srvInterface a ServerInterface object used to communicate with Vertica
     *
     * @note More than one object may be instantiated per query.
     *
     */
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) = 0;

    /// Indicates that this phase is a pre-pass (i.e., runs before any data movement)
    void setPrepass() { prepass = true; }

    bool isPrepass() { return prepass; }

private:
    bool prepass;
};

class MultiPhaseTransformFunctionFactory : public UDXFactory
{
public:
    virtual ~MultiPhaseTransformFunctionFactory() {}

    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        std::vector<TransformFunctionPhase *> phases;
        getPhases(srvInterface, phases);

        SizedColumnTypes phase_itypes = argTypes;
        for (uint i=0; i<phases.size(); i++)
        {
            SizedColumnTypes phase_rtypes;
            if (!phases[i])
                ereport(ERROR, (errmsg("Phase %u cannot be NULL", i)));
            phases[i]->getReturnType(srvInterface, phase_itypes, phase_rtypes);
            phase_itypes = phase_rtypes; // output of this phase is input to next phase
            returnType = phase_rtypes; // update the last phase's rtypes
        }
    }

    /// Returns a vector of pointers to TransformFunctionPhase objects that
    /// represent the various phases of computation
    virtual void getPhases(ServerInterface &srvInterface, std::vector<TransformFunctionPhase *> &phases) = 0;

protected:
    /**
     * @return the object type internally used by Vertica
     */
    virtual UDXType getUDXFactoryType() { return MULTI_TRANSFORM; }

};

/////////////////////////////// User-Defined Loads ////////////////////////////////

class UDLFactory : public UDXFactory
{
public:
    virtual ~UDLFactory() {}

    /**
     * Provides the argument and return types of the UDL.
     * UDL's take no input tuples; as such, their prototype is empty.
     */
    void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType) {}

    /**
     * Not used in this form
     */
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType) {}

};


/**
 * Interface that UDX writers can use to interact with the Vertica Server
 */
class ServerInterface
{
public:
    // Prototype for function to log messages to the Vertica log
    typedef void (*LoggingFunc)(ServerInterface *, const char *fmt, va_list ap);

    /**
     * Create a new ServerInterface.
     *
     * @note that Only Vertica Server should use this method. It is not
     * guaranteed to stay the same between releases.
     */
    ServerInterface(VTAllocator *allocator, 
                    LoggingFunc func, 
                    const std::string &sqlName, 
                    const ParamReader &paramReader)
        : allocator(allocator),
          vlogPtr(func),
          sqlName(sqlName),
          paramReader(paramReader) {}

    // Used for the VSimulator
    ServerInterface(VTAllocator *allocator, LoggingFunc func, const std::string &sqlName)
        : allocator(allocator), vlogPtr(func), sqlName(sqlName) {}

    virtual ~ServerInterface() {}

    /**
     * Returns the ParamReader that allows accessing parameter values using
     * their names
     */
    ParamReader getParamReader() { return paramReader; }

    /**
     * Write a message to the vertica.log system log. The message will contain
     * the SQL name of the user defined function or transform being called
     *
     * @param format a printf style format string specifying the log message
     * format.
     **/
    void log(const char *format, ...) __attribute__((format(printf, 2, 3)))
    {
        std::string formatWithName(sqlName);
        formatWithName.append(" - ");
        formatWithName.append(format);
        format = formatWithName.c_str();
        va_list ap;
        va_start(ap, format);
        vlog(format, ap);
        va_end(ap);
    }

    /**
     * Write a message to the vertica.log system log.
     *
     * @param format a printf style format string specifying the log message
     * format.
     *
     * @param ap va_list for variable arguments
     **/
    void vlog(const char *format, va_list ap) { vlogPtr(this, format, ap); }

    /**
     * Memory source which is managed and freed by the server.
     */
    VTAllocator *allocator;

    /**
     * Set the paramReader of this ServerInterface when delayed creation is required
     * Used by the code when delayed creation of the parameters is needed
     * Users should not call this function
     */
    void setParamReader(const ParamReader &paramReader) {
        this->paramReader = paramReader;
    }

    /**
     * @return the name of the vertica node on which this code is executed.
     */
    const std::string &getCurrentNodeName() const { return nodeName; }

    /**
     * @return the locale of the current session.
     */
    const std::string &getLocale() const          { return locale; }

protected:
    /**
     * Callback for logging, set by the server
     */
    LoggingFunc vlogPtr;

    /**
     * Store the name for error logging
     */
    std::string sqlName;

    /**
     * Store the name of the current node
     */
    std::string nodeName;

    /**
     * A reader for paremeters that have been toknized using the following format:
     * key1=val1,key2=val2,key3=val3. Has accessor methods like BlockReader to be
     * able to access parameters of different data types
     */
    ParamReader paramReader;

    /**
     * The locale of the current session
     */
    std::string locale;

    friend class EE::UserDefinedAnalytic;
    friend class EE::UserDefinedTransform;
    friend class EE::UserDefinedAggregate;
    friend class UdfSupport;
};

/**
 * @brief Iterator interface for writing rows to a Vertica block
 *
 * This class provides the output rows that ScalarFunction.processBlock()
 * writes to.
 */
class ParamWriter : public ParamReader  // All writers should be readable as well
{
public:
    ParamWriter(VTAllocator *allocator = NULL) :
        ParamReader(0), allocator(allocator)
    {}

    /**
     * Setter methods
     */

    /// @brief Adds an INTEGER value to the output row.
    /// @param r The INTEGER value to insert into the output row.
    void setInt(std::string fieldName, vint r)                { addField<vint>(fieldName, r); typeMetaData.addInt(fieldName); }
    /// @brief Adds a FLOAT value to the output row.
    /// @param r The FLOAT value to insert into the output row.
    void setFloat(std::string fieldName, vfloat r)            { addField<vfloat>(fieldName, r); typeMetaData.addFloat(fieldName); }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setBool(std::string fieldName, vbool r)              { addField<vbool>(fieldName, r); typeMetaData.addBool(fieldName); }
    /// @brief Adds a BOOLEAN value to the output row.
    /// @param r The BOOLEAN value to insert into the output row.
    void setDate(std::string fieldName, DateADT r)            { addField<DateADT>(fieldName, r); typeMetaData.addDate(fieldName); }
    /// @brief Adds an INTERVAL value to the output row.
    /// @param r The INTERVAL value to insert into the output row.
    void setInterval(std::string fieldName, Interval r, int32 precision, int32 range) { addField<Interval>(fieldName, r); typeMetaData.addInterval(precision, range, fieldName); }
    /// @brief Adds an INTERVAL YEAR TO MONTH value to the output row.
    /// @param r The INTERVAL YEAR TO MONTH value to insert into the output row.
    void setIntervalYM(std::string fieldName, IntervalYM r, int32 range)              { addField<IntervalYM>(fieldName, r); typeMetaData.addIntervalYM(range, fieldName); }
    /// @brief Adds a TIMESTAMP value to the output row.
    /// @param r The TIMESTAMP value to insert into the output row.
    void setTimestamp(std::string fieldName, Timestamp r, int32 precision)            { addField<Timestamp>(fieldName, r); typeMetaData.addTimestamp(precision, fieldName); }
    /// @brief Adds a TIMESTAMP WITH TIMEZONE value to the output row.
    /// @param r The TIMESTAMP WITH TIMEZONE value to insert into the output row.
    void setTimestampTz(std::string fieldName, TimestampTz r, int32 precision)        { addField<TimestampTz>(fieldName, r); typeMetaData.addTimestampTz(precision, fieldName); }
    /// @brief Adds a TIME value to the output row.
    /// @param r The TIME value to insert into the output row.
    void setTime(std::string fieldName, TimeADT r, int32 precision)                   { addField<TimeADT>(fieldName, r); typeMetaData.addTime(precision, fieldName); }
    /// @brief Adds a TIME WITH TIMEZONE value to the output row.
    /// @param r The TIME WITH TIMEZONE value to insert into the output row.
    void setTimeTz(std::string fieldName, TimeTzADT r, int32 precision)               { addField<TimeTzADT>(fieldName, r); typeMetaData.addTimeTz(precision, fieldName); }

    /// @brief Allocate a new VNumeric object to use as output.
    ///
    /// @return A new VNumeric object to hold output. This object
    ///         automatically added to the output row.
    VNumeric &getNumericRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VNumeric vn(0, 0, 0);
            addFieldNoWrite(fieldName, vn);
            typeMetaData.addNumeric(0, 0, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }

    /// @brief Allocates a new VString object to use as output.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString &getStringRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VString str(0);
            addFieldNoWrite(fieldName, str);
            typeMetaData.addVarchar(65000, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers.at(idx).sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    /// @brief Allocates a new VString object to use as output.
    ///        Sets it to be a 32mb LONG type by default.
    ///
    /// @return A new VString object to hold output. This object
    ///         automatically added to the output row.
    VString &getLongStringRef(std::string fieldName)
    {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) == paramNameToIndex.end()) {
            VString str(0);
            addFieldNoWrite(fieldName, str);
            typeMetaData.addLongVarchar(32768000, fieldName);  // Fake data for now, since we don't actually know
        }

        size_t idx = paramNameToIndex.at(fieldName);
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers.at(idx).sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    /// @brief Sets the allocator to be used to allocate variable size param values
    ///        such as VString. Given allocator live longer than this ParamWriter.
    ///
    /// @param allocator Used to allocate param values.
    void setAllocator(VTAllocator *allocator) {
        this->allocator = allocator;
    }

private:
    VTAllocator *allocator;    

    VNumeric &getNumericRef(size_t idx)
    {
        uint64 *words = getColPtrForWrite<uint64>(idx);
        vnWrappers[idx].words = words;
        return vnWrappers[idx];
    }
    VString &getStringRef(size_t idx)
    {
        EE::StringValue *sv = getColPtrForWrite<EE::StringValue>(idx);
        if (svWrappers[idx].sv != sv) {
            svWrappers[idx].sv = sv;
        }
        return svWrappers[idx];
    }

    template <class T>
    void addField(std::string fieldName, T& datum) {
        addFieldNoWrite(fieldName, datum);
        writeDatum(cols.size()-1LL, datum);
    }

    template <class T>
    void addFieldNoWrite(std::string fieldName, T& datum) {
        std::transform(fieldName.begin(), fieldName.end(), fieldName.begin(), ::tolower);
        if (paramNameToIndex.find(fieldName) != paramNameToIndex.end()) {
            ereport(ERROR,
                    (errmsg("Tried to add field '%s' that already exists", fieldName.c_str()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        paramNameToIndex[fieldName] = cols.size();

        VIAssert(allocator);
        cols.push_back((char*)allocator->alloc(sizeOf(datum)));
        colstrides.push_back(sizeOf(datum));
        vnWrappers.push_back(VNumeric(0, typemodOf(datum)));
        svWrappers.push_back(VString(0));

        if (ncols < cols.size()) ncols = cols.size();
    }

    template <class T> size_t sizeOf(T& datum) { return sizeof(datum); }
    size_t sizeOf(VNumeric& datum) { return 65536; /* max block size */ }
    size_t sizeOf(VString& datum) { return 65536; /* max block size */ }

    template <class T> int32 typemodOf(T& datum) { return VerticaType::makeNumericTypeMod(2,1); }
    int32 typemodOf(VNumeric& datum) { return datum.typmod; }

    template <class T> void writeDatum(size_t colIndex, T& datum) { getColRefForWrite<T>(colIndex) = datum; }
    void writeDatum(size_t colIndex, VNumeric& datum) { getNumericRef(colIndex).copy(&datum); }
    void writeDatum(size_t colIndex, VString& datum) { getStringRef(colIndex).copy(&datum); }

    friend class VerticaBlockSerializer;
};

/**
 * Interface that allows storage of query-plan state,
 * when different parts of query planning take place on different computers.
 * For example, if some work is done on the query initiator node and
 * some is done on each node executing the query.
 */
class PlanContext {
public:
    PlanContext(ParamWriter &writer, std::vector<std::string> clusterNodes)
        : writer(writer), clusterNodes(clusterNodes) {}
    PlanContext(ParamWriter &writer) : writer(writer) {}

    /**
     * Get the current context for writing
     */
    ParamWriter& getWriter() { return writer; }

    /**
     * Get a read-only instance of the current context
     */
    ParamReader& getReader() { return writer; /* ParamWriter is a subclass of ParamReader; ParamReader is the read-only form */ }

    /**
     * Get a list of all of the nodes in the current cluster, by node name
     */
    const std::vector<std::string> &getClusterNodes() { return clusterNodes; }

private:
    ParamWriter &writer;
    std::vector<std::string> clusterNodes;
};

/**
 * Interface that allows storage of query-plan state,
 * when different parts of query planning take place on different computers.
 * For example, if some work is done on the query initiator node and
 * some is done on each node executing the query.
 *
 * In addition to the functionality provided by PlanContext,
 * NodeSpecifyingPlanContext allows you to specify which nodes
 * the query should run on.
 */
class NodeSpecifyingPlanContext : public PlanContext {
public:
    NodeSpecifyingPlanContext(ParamWriter &writer, std::vector<std::string> clusterNodes, std::vector<std::string> targetNodes)
        : PlanContext(writer, clusterNodes), targetNodes(targetNodes) {}
    NodeSpecifyingPlanContext(ParamWriter &writer, std::vector<std::string> clusterNodes)
        : PlanContext(writer, clusterNodes), targetNodes(clusterNodes /* Default to "ALL NODES" */) {}
    NodeSpecifyingPlanContext(ParamWriter &writer) : PlanContext(writer) {}

    /**
     * Return the set of nodes that this query is currently set to run on
     */
    const std::vector<std::string>& getTargetNodes() const { return targetNodes; }

    /**
     * Change the set of nodes that the query is intended to run on.
     * Throws UnknownNodeException if any of the specified node names is not actually the name of any node in the cluster.
     */
    void setTargetNodes(const std::vector<std::string>& nodes) {
        const std::vector<std::string> &knownNodes = this->getClusterNodes();

        // Validate the nodes list.
        for (std::vector<std::string>::const_iterator iter = nodes.begin();
                iter != nodes.end(); ++iter) {
            if (std::find(knownNodes.begin(), knownNodes.end(), *iter) == knownNodes.end()) {
                ereport(ERROR,
                        (errmsg("Tried to add unknown node '%s' to user-defined query plan",
                                iter->c_str()),
                        errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
            }
        }

        targetNodes.assign(nodes.begin(), nodes.end());
    }

private:
    std::vector<std::string> targetNodes;
};


#endif

