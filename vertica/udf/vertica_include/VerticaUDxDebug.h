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
#ifndef VERTICA_UDX_DEBUG_H
#define VERTICA_UDX_DEBUG_H

/*
 * VerticaUDxDebug.h
 *
 * Debugging aids for writing Vertica UDx functions
 */

#include <string.h>
#include <Vertica.h>

namespace Vertica
{

/**
 * Class to formatting the data from a UDF arg reader.
 *
 * See BlockFormatterCout for example
 */
struct BlockFormatter
{
    BlockFormatter(const BlockReader &reader) : _reader(reader) {}
    virtual ~BlockFormatter() {}

    virtual void writeRow(int rowNum, const std::string &row) = 0;

    void format()
    {
        int rowNum = 0;
        do 
        {
            std::stringstream ss; // this is bound to be fast
            for (size_t i=0; i<_reader.getNumCols(); i++)
            {
                if (i != 0) ss << ", ";

                const VerticaType &vt = 
                  _reader.getTypeMetaData().getColumnType(i);
                
                // Format based on type...
                switch (vt.getTypeOid()) 
                {
                  case Int8OID:
                  {
                      ss << _reader.getIntRef(i);
                      break;
                  }

                  case Float8OID: 
                  {
                      ss <<  _reader.getFloatRef(i);
                      break;
                  }

                  case BoolOID:
                  {
                      ss << _reader.getBoolRef(i);
                      break;
                  }
                    
                  case IntervalOID:
                  {
                      //Interval v = _reader.getIntervalRef(i);
                      ss << "<INTERVAL>"; // TODO: Date/Time formatting
                      break;
                  }

                  case IntervalYMOID:
                  {
                      //IntervalYM v = _reader.getIntervalYMRef(i);
                      ss << "<INTERVALYM>"; // TODO: Date/Time formatting
                      break;
                  }

                  case TimestampOID:
                  {
                      //Timestamp v = _reader.getTimestampRef(i);
                      ss << "<TIMESTAMP>"; // TODO: Date/Time formatting
                      break;
                  }
                    
                  case TimestampTzOID: 
                  {
                      //TimestampTz v = _reader.getTimestampTzRef(i);
                      ss << "<TIMESTAMPTS>"; // TODO: Date/Time formatting
                      break;
                  }

                  case NumericOID: 
                  {
                      //VNumeric &num = _reader.getNumericRef(i);
                      ss << "<NUMERIC>"; // TODO: numeric formatting
                      break;
                  }

                  case VarcharOID:
                  {
                      const VString &vstr = _reader.getStringRef(i);
                      if (vstr.isNull()) {
                          ss << "NULL";
                      } else {
                          ss << vstr.str();
                      }
                      break;
                  }

                  default:
                  {
                      ss << "(unimplemented formated output type)";
                  }
                }
            }
            
            writeRow(rowNum++, ss.str()); // write it out and carry on
        } while (_reader.next());
    }

private:
    BlockReader _reader; // copy the reader (avoid advancing the input)
};

/**
 * Formatting rows in a BlockReader to stdout
 *
 * Example Usage with BlockReader *arg_reader
 * 
 * BlockFormatterCout(*arg_reader).format();
 */
struct BlockFormatterCout : public BlockFormatter
{
    BlockFormatterCout(const BlockReader &reader) : BlockFormatter(reader) {}
    virtual ~BlockFormatterCout() {}

    void writeRow(int rowNum, const std::string &row)
    {
        std::cout << row << std::endl;
    }
};

} // namespace

#endif
