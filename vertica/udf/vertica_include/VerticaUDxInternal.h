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
#ifndef VERTICA_UDX_INTERNAL_H
#define VERTICA_UDX_INTERNAL_H

/*
 * Include definitions shared between internal UDx and Vertica server (these
 * are available to UDx writers, but are not documented yet)
 */

#include "VerticaUDx.h"


class IndexedBlockReader
{
public:
    IndexedBlockReader(size_t narg, int rowcount, int *indices)
    : narg(narg), rowcount(rowcount), indices(indices),
      rows_read(0), svWrappers(narg, VString(0))
    {
        args.reserve(narg);
        argstrides.reserve(narg);
        argbases.reserve(narg);
    }

    size_t getNumCols() const { return narg; }

    /** 
     * @return a pointer to the idx'th argument, cast appropriately.
     *
     * Example:
     * const vint *a = arg_reader->getIntPtr(0);
     */
    const vint *getIntPtr(size_t idx)                { return getColPtr<vint>(idx); }
    const vfloat *getFloatPtr(size_t idx)            { return getColPtr<vfloat>(idx); }
    const vbool *getBoolPtr(size_t idx)              { return getColPtr<vbool>(idx); }
    const Interval *getIntervalPtr(size_t idx)       { return getColPtr<Interval>(idx); }
    const IntervalYM *getIntervalYMPtr(size_t idx)   { return getColPtr<IntervalYM>(idx); }
    const Timestamp *getTimestampPtr(size_t idx)     { return getColPtr<Timestamp>(idx); }
    const TimestampTz *getTimestampTzPtr(size_t idx) { return getColPtr<TimestampTz>(idx); }
    const VNumeric *getNumericPtr(size_t idx)    
    { 
        const uint64 *words = reinterpret_cast<const uint64 *>(getColPtr<vint>(idx));
        vnWrappers[idx].words = const_cast<uint64*>(words);
        return &vnWrappers[idx];
    }

    const VString *getStringPtr(size_t idx)    
    {
        const EE::StringValue *sv = reinterpret_cast<const EE::StringValue *>(getColPtr<vint>(idx));
        svWrappers[idx].sv = const_cast<EE::StringValue*>(sv);
        return &svWrappers[idx];
    }

    /** 
     * @return a reference to the idx'th argument, cast appropriately.
     *
     * Example:
     * const vint a = arg_reader->getIntRef(0);
     */
    const vint &getIntRef(size_t idx)                { return *getIntPtr(idx); }
    const vfloat &getFloatRef(size_t idx)            { return *getFloatPtr(idx); }
    const vbool &getBoolRef(size_t idx)              { return *getBoolPtr(idx); }
    const Interval &getIntervalRef(size_t idx)       { return *getIntervalPtr(idx); }
    const IntervalYM &getIntervalYMRef(size_t idx)   { return *getIntervalYMPtr(idx); }
    const Timestamp &getTimestampRef(size_t idx)     { return *getTimestampPtr(idx); }
    const TimestampTz &getTimestampTzRef(size_t idx) { return *getTimestampTzPtr(idx); }
    const VNumeric &getNumericRef(size_t idx)        { return *getNumericPtr(idx); }
    const VString &getStringRef(size_t idx)          { return *getStringPtr(idx); }

    bool next() {
        if (++rows_read >= rowcount)
            return false;

        for (size_t i = 0; i < narg; ++i)
            args[i] = argbases[i] + argstrides[i] * indices[rows_read];
        return true;
    }

private:
    void addCol(int argstride, const char *argbase, const VerticaType &dt)
    {
        args.push_back(argbase + argstride * indices[0]);
        argstrides.push_back(argstride);
        argbases.push_back(argbase);
        int32 typmod = (dt.getTypeOid() == NumericOID
                        ? dt.getTypeMod()
                        : VerticaType::makeNumericTypeMod(2,1));
        vnWrappers.push_back(VNumeric(0, typmod));
    }

    template <class T>
    const T *getColPtr(size_t idx)
    {
        return reinterpret_cast<const T *>(args[idx]);
    }

    template <class T>
    const T &getColRef(size_t idx) { return *getColPtr<T>(idx); }



    std::vector<const char *> args;      // list of pointers that holds the args
    std::vector<int> argstrides;         // list of stride values for each arg
    std::vector<const char *> argbases;  // list of pointers for arg base addrs
    size_t narg;                         // number of arguments
    int rowcount;                        // number of records 
    int *indices;                        // for rows that are not skipped

    int rows_read;                       // number of rows already read

    std::vector<VString> svWrappers;     // pre-create the VString objects to avoid per-call creation
    std::vector<VNumeric> vnWrappers;    // pre-create the VNumeric objects to avoid per-call creation

    friend class EE::VEval;
};


class IndexedBlockWriter
{
public:
    IndexedBlockWriter(char *outArr, int stride, int rowcount, int *indices, const VerticaType &dt)
    : outArr(outArr), stride(stride), indices(indices),
      rows_written(0), svWrapper(0),
      vnWrapper(0, dt.getTypeOid() == NumericOID
                   ? dt.getTypeMod()
                   : VerticaType::makeNumericTypeMod(2, 1))
    {}

    void *getVoidPtr() { return outArr + stride * indices[rows_written]; }

    /**
     * Setter methods
     */
    void setInt(vint r)                { getColRefForWrite<vint>() = r; }
    void setFloat(vfloat r)            { getColRefForWrite<vfloat>() = r; }
    void setBool(vbool r)              { getColRefForWrite<vbool>() = r; }
    void setInterval(Interval r)       { getColRefForWrite<Interval>() = r; }
    void setIntervalYM(IntervalYM r)   { getColRefForWrite<IntervalYM>() = r; }
    void setTimestamp(Timestamp r)     { getColRefForWrite<Timestamp>() = r; }
    void setTimestampTz(TimestampTz r) { getColRefForWrite<TimestampTz>() = r; }

    VNumeric &getNumericRef()          
    { 
        vnWrapper.words = reinterpret_cast<uint64 *>(getVoidPtr());
        return vnWrapper;
    }

    VString &getStringRef()            
    { 
        svWrapper.sv = reinterpret_cast<EE::StringValue *>(getVoidPtr());
        return svWrapper;
    }

    void next()
    {
        ++rows_written;
    }

private:
    template <class T>
    T *getColPtrForWrite()
    {
        return reinterpret_cast<T *>(getVoidPtr());
    }

    template <class T>
    T &getColRefForWrite() { return *getColPtrForWrite<T>(); }

    char *outArr;                       // base addr of output array
    int stride;
    int *indices;                       // for rows that are not skipped

    int rows_written;                   // number of rows already written

    VString svWrapper;                  // pre-create the VString object to avoid per-call creation
    VNumeric vnWrapper;                 // pre-create the VString object to avoid per-call creation
};

/**
 * Specialization of ScalarFunction to allow the user to override the function
 * that only operates on a subset of rows in a block.
 */
class IndexListScalarFunction : public ScalarFunction
{
public:
    virtual void processIndexedBlock(ServerInterface &srvInterface,
                                     IndexedBlockReader &arg_reader,
                                     IndexedBlockWriter &res_writer) = 0;

protected:
    virtual InterfaceType getInterfaceType() { return IndexListFunctionT; }
};



#endif
