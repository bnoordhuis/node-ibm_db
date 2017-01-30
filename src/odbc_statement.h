/*
  Copyright (c) 2013, Dan VerWeire<dverweire@gmail.com>

  Permission to use, copy, modify, and/or distribute this software for any
  purpose with or without fee is hereby granted, provided that the above
  copyright notice and this permission notice appear in all copies.

  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

#ifndef _SRC_ODBC_STATEMENT_H
#define _SRC_ODBC_STATEMENT_H

#include <nan.h>
#include <uv.h>
#include <v8.h>

class ODBCStatement : public Nan::ObjectWrap {
  public:
   static Nan::Persistent<Function> constructor;
   static void Init(v8::Handle<Object> exports);
   
   void Free();
   
  protected:
    ODBCStatement() {};
    
    explicit ODBCStatement(SQLHENV hENV, SQLHDBC hDBC, SQLHSTMT hSTMT): 
      Nan::ObjectWrap(),
      m_hENV(hENV),
      m_hDBC(hDBC),
      m_hSTMT(hSTMT) {};
     
    ~ODBCStatement();

    //constructor
    static NAN_METHOD(New);

    //async methods
    static NAN_METHOD(Bind);
    static NAN_METHOD(Execute);
    static NAN_METHOD(ExecuteDirect);
    static NAN_METHOD(ExecuteNonQuery);
    static NAN_METHOD(Prepare);

    //sync methods
    static NAN_METHOD(CloseSync);
    static NAN_METHOD(ExecuteSync);
    static NAN_METHOD(ExecuteDirectSync);
    static NAN_METHOD(ExecuteNonQuerySync);
    static NAN_METHOD(PrepareSync);
    static NAN_METHOD(BindSync);

  protected:
    friend struct ExecuteJob;
    friend bool CloneBindParams(const Nan::FunctionCallbackInfo<v8::Value>&);

    SQLHENV m_hENV;
    SQLHDBC m_hDBC;
    SQLHSTMT m_hSTMT;
    
    Parameter *params;
    int paramCount;
    
    uint16_t *buffer;
    int bufferLength;
    Column *columns;
    short colCount;
};

#endif
