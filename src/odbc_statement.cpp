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

#include <string.h>
#include <v8.h>
#include <node.h>
#include <node_version.h>
#include <time.h>
#include <uv.h>

#include "odbc.h"
#include "odbc_connection.h"
#include "odbc_result.h"
#include "odbc_statement.h"

#include "async-job.h"

#include <new>  // std::nothrow

using namespace v8;
using namespace node;

Nan::Persistent<Function> ODBCStatement::constructor;

void ODBCStatement::Init(v8::Handle<Object> exports) {
  DEBUG_PRINTF("ODBCStatement::Init\n");
  Nan::HandleScope scope;

  Local<FunctionTemplate> t = Nan::New<FunctionTemplate>(New);

  // Constructor Template
  
  t->SetClassName(Nan::New("ODBCStatement").ToLocalChecked());

  // Reserve space for one Handle<Value>
  Local<ObjectTemplate> instance_template = t->InstanceTemplate();
  instance_template->SetInternalFieldCount(1);
  
  // Prototype Methods
  Nan::SetPrototypeMethod(t, "execute", Execute);
  Nan::SetPrototypeMethod(t, "executeSync", ExecuteSync);
  
  Nan::SetPrototypeMethod(t, "executeDirect", ExecuteDirect);
  Nan::SetPrototypeMethod(t, "executeDirectSync", ExecuteDirectSync);
  
  Nan::SetPrototypeMethod(t, "executeNonQuery", ExecuteNonQuery);
  Nan::SetPrototypeMethod(t, "executeNonQuerySync", ExecuteNonQuerySync);
  
  Nan::SetPrototypeMethod(t, "prepare", Prepare);
  Nan::SetPrototypeMethod(t, "prepareSync", PrepareSync);
  
  Nan::SetPrototypeMethod(t, "bind", Bind);
  Nan::SetPrototypeMethod(t, "bindSync", BindSync);
  
  Nan::SetPrototypeMethod(t, "closeSync", CloseSync);

  // Attach the Database Constructor to the target object
  constructor.Reset(t->GetFunction());
  exports->Set(Nan::New("ODBCStatement").ToLocalChecked(), t->GetFunction());
}

ODBCStatement::~ODBCStatement() {
  this->Free();
}

void ODBCStatement::Free() {
  DEBUG_PRINTF("ODBCStatement::Free paramCount = %i, m_hSTMT =%X\n", paramCount, m_hSTMT);
  //if we previously had parameters, then be sure to free them
  if (paramCount) {
      FREE_PARAMS( params, paramCount ) ;
      DEBUG_PRINTF("ODBCStatement::Free - Params Freed.\n");
  }
  
  if (m_hSTMT) {
    SQLFreeHandle(SQL_HANDLE_STMT, m_hSTMT);
    m_hSTMT = (SQLHSTMT)NULL;
  }
    
  if (bufferLength > 0) {
      if(buffer) free(buffer);
      buffer = NULL;
      bufferLength = 0;
  }
  DEBUG_PRINTF("ODBCStatement::Free() Done.\n");
}

NAN_METHOD(ODBCStatement::New) {
  DEBUG_PRINTF("ODBCStatement::New\n");
  Nan::HandleScope scope;
  
  REQ_EXT_ARG(0, js_henv);
  REQ_EXT_ARG(1, js_hdbc);
  REQ_EXT_ARG(2, js_hstmt);
  
  SQLHENV hENV = static_cast<SQLHENV>((intptr_t)js_henv->Value());
  SQLHDBC hDBC = static_cast<SQLHDBC>((intptr_t)js_hdbc->Value());
  SQLHSTMT hSTMT = static_cast<SQLHSTMT>((intptr_t)js_hstmt->Value());
  
  //create a new OBCResult object
  ODBCStatement* stmt = new ODBCStatement(hENV, hDBC, hSTMT);
  
  //specify the buffer length
  stmt->bufferLength = MAX_VALUE_SIZE;
  
  //initialze a buffer for this object
  stmt->buffer = (uint16_t *) malloc(stmt->bufferLength+2);
  MEMCHECK( stmt->buffer );

  //set the initial colCount to 0
  stmt->colCount = 0;
  
  //initialize the paramCount
  stmt->paramCount = 0;
  stmt->params = 0;
  
  stmt->Wrap(info.Holder());
  
  info.GetReturnValue().Set(info.Holder());
}

struct ExecuteJob : public Job<ExecuteJob, ODBCStatement> {
  enum Mode { BIND, EXECUTE, EXECUTE_DIRECT, EXECUTE_NON_QUERY, PREPARE };

  inline explicit ExecuteJob(Mode mode) : mode(mode), sql(Nan::EmptyString()) {
    assert(mode == BIND || mode == EXECUTE || mode == EXECUTE_NON_QUERY);
  }

  inline explicit ExecuteJob(Mode mode, v8::Local<v8::String> sql)
      : mode(mode), sql(sql) {
    assert(mode == EXECUTE_DIRECT || mode == PREPARE);
  }

  inline void Work(ODBCStatement* stmt) {
    if (mode == BIND) {
      result =
          ODBC::BindParameters(stmt->m_hSTMT, stmt->params, stmt->paramCount);
    } else if (mode == EXECUTE || mode == EXECUTE_NON_QUERY) {
      result = SQLExecute(stmt->m_hSTMT);
    } else if (mode == EXECUTE_DIRECT) {
      result = SQLExecDirect(stmt->m_hSTMT, *sql, sql.length());
    } else if (mode == PREPARE) {
      result = SQLPrepare(stmt->m_hSTMT, *sql, sql.length());
    } else {
      UNREACHABLE();
    }

    if (!SQL_SUCCEEDED(result)) {
      RecordSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT);
    }
  }

  inline void Done(ODBCStatement* stmt) {
    if (SQL_SUCCEEDED(result)) {
      if (mode == BIND || mode == PREPARE) {
        Local<Value> info[2];
        info[0] = Nan::Null();
        info[1] = Nan::True();
        MakeCallback(arraysize(info), info);
      } else if (mode == EXECUTE) {
        v8::Local<v8::Value> info[3];
        info[0] = Nan::Null();
        info[1] = NewODBCResult(stmt);
        info[2] = GetOutputParameters(stmt);
        if (info[2].IsEmpty()) info[2] = Nan::Null();
        FREE_PARAMS(stmt->params, stmt->paramCount);
        MakeCallback(arraysize(info), info);
      } else if (mode == EXECUTE_DIRECT) {
        v8::Local<v8::Value> info[2];
        info[0] = Nan::Null();
        info[1] = NewODBCResult(stmt);
        MakeCallback(arraysize(info), info);
      } else if (mode == EXECUTE_NON_QUERY) {
        Local<Value> info[2];
        info[0] = Nan::Null();
        info[1] = Nan::New<v8::Number>(CountRowsAndClose(stmt));
        MakeCallback(arraysize(info), info);
      } else {
        UNREACHABLE();
      }
    } else {
      if (mode == EXECUTE) FREE_PARAMS(stmt->params, stmt->paramCount);
      MakeCallback(MakeSQLError());
    }
  }

  inline void DoneSync(ODBCStatement* stmt,
                       const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (SQL_SUCCEEDED(result)) {
      if (mode == BIND || mode == PREPARE) {
        info.GetReturnValue().Set(true);
      } else if (mode == EXECUTE) {
        v8::Local<v8::Object> object = NewODBCResult(stmt);
        v8::Local<v8::Array> outparams = GetOutputParameters(stmt);
        if (outparams.IsEmpty()) {
          info.GetReturnValue().Set(object);
        } else {
          v8::Local<v8::Array> rval = Nan::New<v8::Array>(2);
          rval->Set(0, object);
          rval->Set(1, outparams);
          info.GetReturnValue().Set(rval);
        }
      } else if (mode == EXECUTE_DIRECT) {
        info.GetReturnValue().Set(NewODBCResult(stmt));
      } else if (mode == EXECUTE_NON_QUERY) {
        info.GetReturnValue().Set(CountRowsAndClose(stmt));
      } else {
        UNREACHABLE();
      }
    } else {
      Nan::ThrowError(MakeSQLError());
    }
    if (mode == EXECUTE) FREE_PARAMS(stmt->params, stmt->paramCount);
  }

  inline v8::Local<v8::Array> GetOutputParameters(ODBCStatement* stmt) {
    assert(mode == EXECUTE);
    v8::Local<v8::Array> outparams;
    for (int i = 0; i < stmt->paramCount; i++) {
      Parameter* param = &stmt->params[i];
      if (param->paramtype % 2 != 0) {
        continue;  // Not a stored procedure result.
      }
      if (outparams.IsEmpty()) {
        outparams = Nan::New<v8::Array>();
      }
      outparams->Set(outparams->Length(), ODBC::GetOutputParameter(*param));
    }
    return outparams;
  }

  inline SQLLEN CountRowsAndClose(ODBCStatement* stmt) {
    assert(mode == EXECUTE_NON_QUERY);
    SQLLEN rows = 0;
    SQLRETURN ret = SQLRowCount(stmt->m_hSTMT, &rows);
    if (!SQL_SUCCEEDED(ret)) {
      rows = 0;
    }
    // FIXME(bnoordhuis) What if SQLFreeStmt() errors?
    SQLFreeStmt(stmt->m_hSTMT, SQL_CLOSE);
    return rows;
  }

  inline v8::Local<v8::Object> NewODBCResult(ODBCStatement* stmt) {
    assert(mode == EXECUTE || mode == EXECUTE_NON_QUERY);
    v8::Local<v8::Value> info[4];
    info[0] = ToExternal(stmt->m_hENV);
    info[1] = ToExternal(stmt->m_hDBC);
    info[2] = ToExternal(stmt->m_hSTMT);
    info[3] = Nan::False();
    return NewInstance(ODBCConnection::constructor, arraysize(info), info);
  }

  const Mode mode;
#ifdef UNICODE  // FIXME(bnoordhuis) Retain std::nothrow behavior on OOM?
  v8::String::Value sql;
#else
  Nan::Utf8String sql;
#endif
  SQLRETURN result;
};

NAN_METHOD(ODBCStatement::Execute) {
  static const int kCallbackIndex = 0;
  ExecuteJob* that = new(std::nothrow) ExecuteJob(ExecuteJob::EXECUTE);
  ExecuteJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCStatement::ExecuteSync) {
  ExecuteJob that(ExecuteJob::EXECUTE);
  ExecuteJob::Sync(HERE, info, &that);
}

NAN_METHOD(ODBCStatement::ExecuteNonQuery) {
  static const int kCallbackIndex = 0;
  ExecuteJob* that =
      new(std::nothrow) ExecuteJob(ExecuteJob::EXECUTE_NON_QUERY);
  ExecuteJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCStatement::ExecuteNonQuerySync) {
  ExecuteJob that(ExecuteJob::EXECUTE_NON_QUERY);
  ExecuteJob::Sync(HERE, info, &that);
}

NAN_METHOD(ODBCStatement::ExecuteDirect) {
  REQ_STRO_ARG(0, sql);
  static const int kCallbackIndex = 1;
  ExecuteJob* that =
      new(std::nothrow) ExecuteJob(ExecuteJob::EXECUTE_DIRECT, sql);
  ExecuteJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCStatement::ExecuteDirectSync) {
  REQ_STRO_ARG(0, sql);
  ExecuteJob that(ExecuteJob::EXECUTE_DIRECT, sql);
  ExecuteJob::Sync(HERE, info, &that);
}

NAN_METHOD(ODBCStatement::Prepare) {
  REQ_STRO_ARG(0, sql);
  static const int kCallbackIndex = 1;
  ExecuteJob* that = new(std::nothrow) ExecuteJob(ExecuteJob::PREPARE, sql);
  ExecuteJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCStatement::PrepareSync) {
  REQ_STRO_ARG(0, sql);
  ExecuteJob that(ExecuteJob::PREPARE, sql);
  ExecuteJob::Sync(HERE, info, &that);
}

inline bool CloneBindParams(const Nan::FunctionCallbackInfo<v8::Value>& info) {
  if (!info[0]->IsArray()) {
    Nan::ThrowTypeError("Argument 1 must be an Array");
    return false;
  }
  ODBCStatement* stmt = Nan::ObjectWrap::Unwrap<ODBCStatement>(info.Holder());
  FREE_PARAMS(stmt->params, stmt->paramCount);
  stmt->params =
      ODBC::GetParametersFromArray(info[0].As<v8::Array>(), &stmt->paramCount);
  return true;
}

NAN_METHOD(ODBCStatement::Bind) {
  if (!CloneBindParams(info)) return;
  static const int kCallbackIndex = 1;
  ExecuteJob* that = new(std::nothrow) ExecuteJob(ExecuteJob::BIND);
  ExecuteJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCStatement::BindSync) {
  if (!CloneBindParams(info)) return;
  ExecuteJob that(ExecuteJob::BIND);
  ExecuteJob::Sync(HERE, info, &that);
}

NAN_METHOD(ODBCStatement::CloseSync) {
  DEBUG_PRINTF("ODBCStatement::CloseSync\n");

  OPT_INT_ARG(0, closeOption, SQL_DESTROY);
  
  ODBCStatement* stmt = Nan::ObjectWrap::Unwrap<ODBCStatement>(info.Holder());
  
  DEBUG_PRINTF("ODBCStatement::CloseSync closeOption=%i\n", 
               closeOption);
  
  if (closeOption == SQL_DESTROY) {
    stmt->Free();
  }
  else {
    SQLFreeStmt(stmt->m_hSTMT, closeOption);
  }

  info.GetReturnValue().Set(true);
}
