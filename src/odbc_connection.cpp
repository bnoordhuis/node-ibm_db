/*
  Copyright (c) 2013, Dan VerWeire <dverweire@gmail.com>
  Copyright (c) 2010, Lee Smith<notwink@gmail.com>

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

Nan::Persistent<Function> ODBCConnection::constructor;
Nan::Persistent<String> ODBCConnection::OPTION_SQL;
Nan::Persistent<String> ODBCConnection::OPTION_PARAMS;
Nan::Persistent<String> ODBCConnection::OPTION_NORESULTS;

void ODBCConnection::Init(v8::Handle<Object> exports) {
  DEBUG_PRINTF("ODBCConnection::Init\n");
  Nan::HandleScope scope;

  OPTION_SQL.Reset(Nan::New<String>("sql").ToLocalChecked());
  OPTION_PARAMS.Reset(Nan::New<String>("params").ToLocalChecked());
  OPTION_NORESULTS.Reset(Nan::New<String>("noResults").ToLocalChecked());

  Local<FunctionTemplate> constructor_template = Nan::New<FunctionTemplate>(New);

  // Constructor Template
  constructor_template->SetClassName(Nan::New("ODBCConnection").ToLocalChecked());

  // Reserve space for one Handle<Value>
  Local<ObjectTemplate> instance_template = constructor_template->InstanceTemplate();
  instance_template->SetInternalFieldCount(1);
  
  // Properties
  //Nan::SetAccessor(instance_template, Nan::New("mode").ToLocalChecked(), ModeGetter, ModeSetter);
  Nan::SetAccessor(instance_template, Nan::New("connected").ToLocalChecked(), ConnectedGetter);
  Nan::SetAccessor(instance_template, Nan::New("connectTimeout").ToLocalChecked(), ConnectTimeoutGetter, ConnectTimeoutSetter);
  
  // Prototype Methods
  Nan::SetPrototypeMethod(constructor_template, "open", Open);
  Nan::SetPrototypeMethod(constructor_template, "openSync", OpenSync);
  Nan::SetPrototypeMethod(constructor_template, "close", Close);
  Nan::SetPrototypeMethod(constructor_template, "closeSync", CloseSync);
  Nan::SetPrototypeMethod(constructor_template, "createStatement", CreateStatement);
  Nan::SetPrototypeMethod(constructor_template, "createStatementSync", CreateStatementSync);
  Nan::SetPrototypeMethod(constructor_template, "query", Query);
  Nan::SetPrototypeMethod(constructor_template, "querySync", QuerySync);
  
  Nan::SetPrototypeMethod(constructor_template, "beginTransaction", BeginTransaction);
  Nan::SetPrototypeMethod(constructor_template, "beginTransactionSync", BeginTransactionSync);
  Nan::SetPrototypeMethod(constructor_template, "endTransaction", EndTransaction);
  Nan::SetPrototypeMethod(constructor_template, "endTransactionSync", EndTransactionSync);

  Nan::SetPrototypeMethod(constructor_template, "setIsolationLevel", SetIsolationLevel);
  
  Nan::SetPrototypeMethod(constructor_template, "columns", Columns);
  Nan::SetPrototypeMethod(constructor_template, "tables", Tables);
  
  // Attach the Database Constructor to the target object
  constructor.Reset(constructor_template->GetFunction());
  exports->Set( Nan::New("ODBCConnection").ToLocalChecked(), constructor_template->GetFunction());
}

ODBCConnection::~ODBCConnection() {
  DEBUG_PRINTF("ODBCConnection::~ODBCConnection\n");
  this->Free();
}

void ODBCConnection::Free() {
  DEBUG_PRINTF("ODBCConnection::Free m_hDBC = %i \n", m_hDBC);
  if (m_hDBC) {
    SQLDisconnect(m_hDBC);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hDBC);
    m_hDBC = (SQLHDBC)NULL;
  }
}

/*
 * New
 */

NAN_METHOD(ODBCConnection::New) {
  DEBUG_PRINTF("ODBCConnection::New\n");
  Nan::HandleScope scope;
  
  REQ_EXT_ARG(0, js_henv);
  REQ_EXT_ARG(1, js_hdbc);
  
  SQLHENV hENV = static_cast<SQLHENV>((intptr_t)js_henv->Value());
  SQLHDBC hDBC = static_cast<SQLHDBC>((intptr_t)js_hdbc->Value());
  
  ODBCConnection* conn = new ODBCConnection(hENV, hDBC);
  
  conn->Wrap(info.Holder());
  
  //set default connectTimeout to 30 seconds
  conn->connectTimeout = 30;
  
  info.GetReturnValue().Set(info.Holder());
}

NAN_GETTER(ODBCConnection::ConnectedGetter) {
  Nan::HandleScope scope;

  ODBCConnection *obj = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

  info.GetReturnValue().Set(obj->connected);
}

NAN_GETTER(ODBCConnection::ConnectTimeoutGetter) {
  Nan::HandleScope scope;

  ODBCConnection *obj = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

  info.GetReturnValue().Set(obj->connectTimeout);
}

NAN_SETTER(ODBCConnection::ConnectTimeoutSetter) {
  Nan::HandleScope scope;

  ODBCConnection *obj = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  if (value->IsNumber()) {
    obj->connectTimeout = value->Uint32Value();
  }
}

struct OpenJob : public Job<OpenJob, ODBCConnection> {
  inline explicit OpenJob(v8::Local<v8::String> connection_string)
      : connection_string(connection_string) {}

  inline void Work(ODBCConnection* conn) {
    if (conn->connectTimeout > 0) {
      SQLUINTEGER timeout = conn->connectTimeout;

      result =
          SQLSetConnectAttr(conn->m_hDBC, SQL_ATTR_LOGIN_TIMEOUT,
                            &timeout, sizeof(timeout));

      if (!SQL_SUCCEEDED(result)) {
        return RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
      }
    }

    result =
        SQLDriverConnect(conn->m_hDBC, NULL,
                         *connection_string, connection_string.length(),
                         NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

    if (!SQL_SUCCEEDED(result)) {
      return RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
    }

    conn->connected = SQL_SUCCEEDED(result);
  }

  inline void Done(ODBCConnection* conn) {
    if (SQL_SUCCEEDED(result)) {
      MakeCallback();
    } else {
      MakeCallback(MakeSQLError());
    }
  }

  inline void DoneSync(ODBCConnection* conn,
                       const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (SQL_SUCCEEDED(result)) {
      info.GetReturnValue().Set(true);
    } else {
      Nan::ThrowError(MakeSQLError());
    }
  }

#ifdef UNICODE  // FIXME(bnoordhuis) Retain std::nothrow behavior on OOM?
  v8::String::Value connection_string;
#else
  Nan::Utf8String connection_string;
#endif
  SQLRETURN result;
};

NAN_METHOD(ODBCConnection::Open) {
  REQ_STRO_ARG(0, connection_string);
  static const int kCallbackIndex = 1;
  OpenJob* that = new(std::nothrow) OpenJob(connection_string);
  OpenJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCConnection::OpenSync) {
  REQ_STRO_ARG(0, connection_string);
  OpenJob that(connection_string);
  OpenJob::Sync(HERE, info, &that);
}

struct CloseJob : public Job<CloseJob, ODBCConnection> {
  inline void Work(ODBCConnection* conn) {
    // TODO(bnoordhuis) Check if there are still in-flight statements.
    conn->Free();
    conn->connected = false;
  }

  inline void Done(ODBCConnection* conn) {
    MakeCallback();
  }

  inline void DoneSync(ODBCConnection* conn,
                       const Nan::FunctionCallbackInfo<v8::Value>& info) {
    info.GetReturnValue().Set(true);
  }
};

NAN_METHOD(ODBCConnection::Close) {
  static const int kCallbackIndex = 0;
  CloseJob* that = new(std::nothrow) CloseJob();
  CloseJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCConnection::CloseSync) {
  CloseJob that;
  CloseJob::Sync(HERE, info, &that);
}

struct CreateStatementJob : public Job<CreateStatementJob, ODBCConnection> {
  inline void Work(ODBCConnection* conn) {
    result = SQLAllocHandle(SQL_HANDLE_STMT, conn->m_hDBC, &hSTMT);
    if (!SQL_SUCCEEDED(result)) RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
  }

  inline void Done(ODBCConnection* conn) {
    v8::Local<v8::Value> info[2];
    if (SQL_SUCCEEDED(result)) {
      info[0] = Nan::Null();
      info[1] = NewODBCStatement(conn);
    } else {
      info[0] = MakeSQLError();
      info[1] = Nan::Null();
    }
    MakeCallback(arraysize(info), info);
  }

  inline void DoneSync(ODBCConnection* conn,
                       const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (SQL_SUCCEEDED(result)) {
      info.GetReturnValue().Set(NewODBCStatement(conn));
    } else {
      Nan::ThrowError(MakeSQLError());
    }
  }

  inline v8::Local<v8::Object> NewODBCStatement(ODBCConnection* conn) {
    v8::Local<v8::Value> info[3];
    info[0] = ToExternal(conn->m_hENV);
    info[1] = ToExternal(conn->m_hDBC);
    info[2] = ToExternal(hSTMT);
    return NewInstance(ODBCStatement::constructor, arraysize(info), info);
  }

  SQLHSTMT hSTMT;
  SQLRETURN result;
};

NAN_METHOD(ODBCConnection::CreateStatement) {
  static const int kCallbackIndex = 0;
  CreateStatementJob* that = new(std::nothrow) CreateStatementJob();
  CreateStatementJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCConnection::CreateStatementSync) {
  CreateStatementJob that;
  CreateStatementJob::Sync(HERE, info, &that);
}

/*
 * Query
 */

NAN_METHOD(ODBCConnection::Query) {
  DEBUG_PRINTF("ODBCConnection::Query\n");
  Nan::HandleScope scope;
  
  Local<Function> cb;
  
  Local<String> sql;
  
  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));
  MEMCHECK( data ) ;

  //Check arguments for different variations of calling this function
  if (info.Length() == 3) {
    //handle Query("sql string", [params], function cb () {});
    
    if ( !info[0]->IsString() ) {
      return Nan::ThrowTypeError("Argument 0 must be an String.");
    }
    else if ( !info[1]->IsArray() ) {
      return Nan::ThrowTypeError("Argument 1 must be an Array.");
    }
    else if ( !info[2]->IsFunction() ) {
      return Nan::ThrowTypeError("Argument 2 must be a Function.");
    }

    sql = info[0]->ToString();
    
    data->params = ODBC::GetParametersFromArray(
      Local<Array>::Cast(info[1]),
      &data->paramCount);
    
    cb = Local<Function>::Cast(info[2]);
  }
  else if (info.Length() == 2 ) {
    //handle either Query("sql", cb) or Query({ settings }, cb)
    
    if (!info[1]->IsFunction()) {
      return Nan::ThrowTypeError("ODBCConnection::Query(): Argument 1 must be a Function.");
    }
    
    cb = Local<Function>::Cast(info[1]);
    
    if (info[0]->IsString()) {
      //handle Query("sql", function cb () {})
      
      sql = info[0]->ToString();
      
      data->paramCount = 0;
    }
    else if (info[0]->IsObject()) {
      //NOTE: going forward this is the way we should expand options
      //rather than adding more arguments to the function signature.
      //specify options on an options object.
      //handle Query({}, function cb () {});
      
      Local<Object> obj = info[0]->ToObject();
      
      Local<String> optionSqlKey = Nan::New(OPTION_SQL);
      if (obj->Has(optionSqlKey) && obj->Get(optionSqlKey)->IsString()) {
        sql = obj->Get(optionSqlKey)->ToString();
      }
      else {
        sql = Nan::New("").ToLocalChecked();
      }
      
      Local<String> optionParamsKey = Nan::New(OPTION_PARAMS);
      if (obj->Has(optionParamsKey) && obj->Get(optionParamsKey)->IsArray()) {
        data->params = ODBC::GetParametersFromArray(
          Local<Array>::Cast(obj->Get(optionParamsKey)),
          &data->paramCount);
      }
      else {
        data->paramCount = 0;
      }
      
      Local<String> optionNoResultsKey = Nan::New(OPTION_NORESULTS);
      if (obj->Has(optionNoResultsKey) && obj->Get(optionNoResultsKey)->IsBoolean()) {
        data->noResultObject = obj->Get(optionNoResultsKey)->ToBoolean()->Value();
      }
      else {
        data->noResultObject = false;
      }
    }
    else {
      return Nan::ThrowTypeError("ODBCConnection::Query(): Argument 0 must be a String or an Object.");
    }
  }
  else {
    return Nan::ThrowTypeError("ODBCConnection::Query(): Requires either 2 or 3 Arguments. ");
  }
  //Done checking arguments

  data->cb = new Nan::Callback(cb);
  data->sqlLen = sql->Length();

#ifdef UNICODE
  data->sqlSize = (data->sqlLen * sizeof(uint16_t)) + sizeof(uint16_t);
  data->sql = (uint16_t *) malloc(data->sqlSize);
  MEMCHECK( data->sql ) ;
  sql->Write((uint16_t *) data->sql);
#else
  data->sqlSize = sql->Utf8Length() + 1;
  data->sql = (char *) malloc(data->sqlSize);
  MEMCHECK( data->sql ) ;
  sql->WriteUtf8((char *) data->sql);
#endif

  DEBUG_PRINTF("ODBCConnection::Query : sqlLen=%i, sqlSize=%i, sql=%s, hDBC=%X\n",
               data->sqlLen, data->sqlSize, (char*) data->sql, conn->m_hDBC);
  
  data->conn = conn;
  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(),
    work_req, 
    UV_Query, 
    (uv_after_work_cb)UV_AfterQuery);

  conn->Ref();

  info.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Query(uv_work_t* req) {
  query_work_data* data = (query_work_data *)(req->data);
  SQLRETURN ret;
  DEBUG_PRINTF("ODBCConnection::UV_Query hDBC=%X\n", data->conn->m_hDBC);
  
  //allocate a new statment handle
  SQLAllocHandle( SQL_HANDLE_STMT, 
                  data->conn->m_hDBC, 
                  &data->hSTMT );

  //check to see if should excute a direct or a parameter bound query
  if (!data->paramCount) {
    // execute the query directly
    ret = SQLExecDirect(
      data->hSTMT,
      (SQLTCHAR *) data->sql, 
      data->sqlLen);
  }
  else {
    // prepare statement, bind parameters and execute statement 
    ret = SQLPrepare(
      data->hSTMT,
      (SQLTCHAR *) data->sql, 
      data->sqlLen);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {

      ret = ODBC::BindParameters( data->hSTMT, data->params, data->paramCount ) ;

      if (SQL_SUCCEEDED(ret)) {
        ret = SQLExecute(data->hSTMT);
      }
    }
  }

  // this will be checked later in UV_AfterQuery
  data->result = ret;
  DEBUG_PRINTF("ODBCConnection::UV_Query done for hDBC=%X\n", data->conn->m_hDBC);
}

void ODBCConnection::UV_AfterQuery(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterQuery\n");
  
  Nan::HandleScope scope;
  
  query_work_data* data = (query_work_data *)(req->data);
  Local<Array> sp_result = Nan::New<Array>();
  int outParamCount = 0; // Non-zero tells its a SP with OUT param

  Nan::TryCatch try_catch;

  DEBUG_PRINTF("ODBCConnection::UV_AfterQuery : data->result=%i, data->noResultObject=%i, stmt=%X\n", 
          data->result, data->noResultObject, data->hSTMT);

  // Retrieve values of INOUT and OUTPUT Parameters of Stored Procedure
  if (SQL_SUCCEEDED(data->result)) {
      for(int i = 0; i < data->paramCount; i++) {
          if(data->params[i].paramtype % 2 == 0) {
              sp_result->Set(Nan::New(outParamCount), ODBC::GetOutputParameter(data->params[i]));
              outParamCount++;
          }
      }
      DEBUG_PRINTF("ODBCConnection::UV_AfterQuery : outParamCount=%i\n", outParamCount);
  }
  if (data->result != SQL_ERROR && data->noResultObject) {
    //We have been requested to not create a result object
    //this means we should release the handle now and call back
    //with Nan::True()
    
    DEBUG_PRINTF("Going to free handle.\n");
    SQLFreeHandle(SQL_HANDLE_STMT, data->hSTMT);
    data->hSTMT = (SQLHSTMT)NULL;
    DEBUG_PRINTF("Handle freed.\n");
    
    Local<Value> info[2];
    info[0] = Nan::Null();
    if(outParamCount) info[1] = sp_result;
    else info[1] = Nan::Null();
    
    DEBUG_PRINTF("Calling callback function...\n");
    data->cb->Call(2, info);
  }
  else {
    Local<Value> info[4];

    info[0] = Nan::New<External>((void*)(intptr_t)data->conn->m_hENV);
    info[1] = Nan::New<External>((void*)(intptr_t)data->conn->m_hDBC);
    info[2] = Nan::New<External>((void*)(intptr_t)data->hSTMT);
    info[3] = Nan::True();
    
    Local<Object> js_result = NewInstance(ODBCResult::constructor, 4, info);

    // Check now to see if there was an error (as there may be further result sets)
    if (data->result == SQL_ERROR) {
      info[0] = ODBC::GetSQLError(SQL_HANDLE_STMT, data->hSTMT, (char *) "[node-ibm_db] SQL_ERROR");
    } else {
      info[0] = Nan::Null();
    }
    info[1] = js_result;
    if(outParamCount) info[2] = sp_result; // Must a CALL stmt
    else info[2] = Nan::Null();
    
    data->cb->Call(3, info);
  }
  
  DEBUG_PRINTF("After callback function executed.\n");
  data->conn->Unref();
  
  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }
  
  delete data->cb;

  if (data->paramCount) {
      FREE_PARAMS( data->params, data->paramCount ) ;
  }

  free(data->sql);
  free(data->catalog);
  free(data->schema);
  free(data->table);
  free(data->type);
  free(data->column);
  free(data);
  free(req);
  
  //scope.Close(Undefined());
}


/*
 * QuerySync
 */

NAN_METHOD(ODBCConnection::QuerySync) {
  DEBUG_PRINTF("ODBCConnection::QuerySync\n");
  Nan::HandleScope scope;

#ifdef UNICODE
  String::Value* sql;
#else
  String::Utf8Value* sql;
#endif

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  Parameter* params = new Parameter[0];
  SQLRETURN ret;
  SQLHSTMT hSTMT;
  int paramCount = 0;
  int outParamCount = 0; // Non-zero tells its a SP.
  Local<Array> sp_result = Nan::New<Array>();
  bool noResultObject = false;
  
  //Check arguments for different variations of calling this function
  if (info.Length() == 2) {
    if ( !info[0]->IsString() ) {
      return Nan::ThrowTypeError("ODBCConnection::QuerySync(): Argument 0 must be an String.");
    }
    else if (!info[1]->IsArray()) {
      return Nan::ThrowTypeError("ODBCConnection::QuerySync(): Argument 1 must be an Array.");
    }

#ifdef UNICODE
    sql = new String::Value(info[0]->ToString());
#else
    sql = new String::Utf8Value(info[0]->ToString());
#endif

    params = ODBC::GetParametersFromArray(
      Local<Array>::Cast(info[1]),
      &paramCount);

  }
  else if (info.Length() == 1 ) {
    //handle either QuerySync("sql") or QuerySync({ settings })

    if (info[0]->IsString()) {
      //handle Query("sql")
#ifdef UNICODE
      sql = new String::Value(info[0]->ToString());
#else
      sql = new String::Utf8Value(info[0]->ToString());
#endif
    
      paramCount = 0;
    }
    else if (info[0]->IsObject()) {
      //NOTE: going forward this is the way we should expand options
      //rather than adding more arguments to the function signature.
      //specify options on an options object.
      //handle Query({}, function cb () {});
      
      Local<Object> obj = info[0]->ToObject();
      
      Local<String> optionSqlKey = Nan::New<String>(OPTION_SQL);
      if (obj->Has(optionSqlKey) && obj->Get(optionSqlKey)->IsString()) {
#ifdef UNICODE
        sql = new String::Value(obj->Get(optionSqlKey)->ToString());
#else
        sql = new String::Utf8Value(obj->Get(optionSqlKey)->ToString());
#endif
      }
      else {
#ifdef UNICODE
        sql = new String::Value(Nan::New("").ToLocalChecked());
#else
        sql = new String::Utf8Value(Nan::New("").ToLocalChecked());
#endif
      }

      Local<String> optionParamsKey = Nan::New(OPTION_PARAMS);
      if (obj->Has(optionParamsKey) && obj->Get(optionParamsKey)->IsArray()) {
        params = ODBC::GetParametersFromArray(
          Local<Array>::Cast(obj->Get(optionParamsKey)),
          &paramCount);
      }
      else {
        paramCount = 0;
      }
      
      Local<String> optionNoResultsKey = Nan::New(OPTION_NORESULTS);
      if (obj->Has(optionNoResultsKey) && obj->Get(optionNoResultsKey)->IsBoolean()) {
        noResultObject = obj->Get(optionNoResultsKey)->ToBoolean()->Value();
        DEBUG_PRINTF("ODBCConnection::QuerySync - under if noResultObject=%i\n", noResultObject);
      }
      else {
        noResultObject = false;
      }
    }
    else {
      return Nan::ThrowTypeError("ODBCConnection::QuerySync(): Argument 0 must be a String or an Object.");
    }
  }
  else {
    return Nan::ThrowTypeError("ODBCConnection::QuerySync(): Requires either 1 or 2 Arguments.");
  }
  //Done checking arguments

  //allocate a new statment handle
  ret = SQLAllocHandle( SQL_HANDLE_STMT, 
                  conn->m_hDBC, 
                  &hSTMT );

  DEBUG_PRINTF("ODBCConnection::QuerySync - hSTMT=%X, noResultObject=%i\n", hSTMT, noResultObject);
  //check to see if should excute a direct or a parameter bound query
  if (!SQL_SUCCEEDED(ret)) {
    //We'll check again later
  }
  else if (!paramCount) {
    // execute the query directly
    ret = SQLExecDirect(
      hSTMT,
      (SQLTCHAR *) **sql, 
      sql->length());
  }
  else {
    // prepare statement, bind parameters and execute statement
    ret = SQLPrepare(
      hSTMT,
      (SQLTCHAR *) **sql, 
      sql->length());
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
      ret = ODBC::BindParameters( hSTMT, params, paramCount ) ;
      if (SQL_SUCCEEDED(ret)) {
        ret = SQLExecute(hSTMT);
        if (SQL_SUCCEEDED(ret)) {
          for(int i = 0; i < paramCount; i++) { // For stored Procedure CALL
            if(params[i].paramtype % 2 == 0) {
              sp_result->Set(Nan::New(outParamCount), ODBC::GetOutputParameter(params[i]));
              outParamCount++;
            }
          }
        }
      }
    }
    FREE_PARAMS( params, paramCount ) ;
  }
  
  delete sql;
  
  //check to see if there was an error during execution
  if (ret == SQL_ERROR) {
    //Free stmt handle and then throw error.
    Local<Value> err = ODBC::GetSQLError(
      SQL_HANDLE_STMT,
      hSTMT,
      (char *) "[node-ibm_db] Error in ODBCConnection::QuerySync while executing query."
    );
    SQLFreeHandle(SQL_HANDLE_STMT, hSTMT);
    hSTMT = (SQLHSTMT)NULL;
    Nan::ThrowError(err);
    return;
  }
  else if (noResultObject) {
    //if there is not result object requested then
    //we must destroy the STMT ourselves.
    SQLFreeHandle(SQL_HANDLE_STMT, hSTMT);
    hSTMT = (SQLHSTMT)NULL;

    if( outParamCount ) // Its a CALL stmt with OUT params.
    { // Return an array with outparams as second element.
      Local<Array> resultset = Nan::New<Array>();
      resultset->Set(0, Nan::Null());
      resultset->Set(1, sp_result);
      info.GetReturnValue().Set(resultset);
    } else {
      info.GetReturnValue().SetNull();
    }
  }
  else {
    Local<Value> result[4];

    result[0] = Nan::New<External>((void*) (intptr_t) conn->m_hENV);
    result[1] = Nan::New<External>((void*) (intptr_t) conn->m_hDBC);
    result[2] = Nan::New<External>((void*) (intptr_t) hSTMT);
    result[3] = Nan::True();
    
    Local<Object> js_result = NewInstance(ODBCResult::constructor, 4, result);

    if( outParamCount ) // Its a CALL stmt with OUT params.
    { // Return an array with outparams as second element. [result, outparams]
      Local<Array> resultset = Nan::New<Array>();
      resultset->Set(0, js_result);
      resultset->Set(1, sp_result);
      info.GetReturnValue().Set(resultset);
    } else {
      info.GetReturnValue().Set(js_result);
    }
  }
}

/*
 * Tables
 */

NAN_METHOD(ODBCConnection::Tables) {
  Nan::HandleScope scope;

  REQ_STRO_OR_NULL_ARG(0, catalog);
  REQ_STRO_OR_NULL_ARG(1, schema);
  REQ_STRO_OR_NULL_ARG(2, table);
  REQ_STRO_OR_NULL_ARG(3, type);
  Local<Function> cb = Local<Function>::Cast(info[4]);

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  query_work_data* data = 
    (query_work_data *) calloc(1, sizeof(query_work_data));
  MEMCHECK( data ) ;
  
  data->sql = NULL;
  data->catalog = NULL;
  data->schema = NULL;
  data->table = NULL;
  data->type = NULL;
  data->column = NULL;
  data->cb = new Nan::Callback(cb);

  if (!catalog->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->catalog = (uint16_t *) malloc((catalog->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->catalog ) ;
    catalog->Write((uint16_t *) data->catalog);
#else
    data->catalog = (char *) malloc(catalog->Length() + 1);
    MEMCHECK( data->catalog ) ;
    catalog->WriteUtf8((char *) data->catalog);
#endif
  }

  if (!schema->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->schema = (uint16_t *) malloc((schema->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->schema ) ;
    schema->Write((uint16_t *) data->schema);
#else
    data->schema = (char *) malloc(schema->Length() + 1);
    MEMCHECK( data->schema ) ;
    schema->WriteUtf8((char *) data->schema);
#endif
  }
  
  if (!table->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->table = (uint16_t *) malloc((table->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->table ) ;
    table->Write((uint16_t *) data->table);
#else
    data->table = (char *) malloc(table->Length() + 1);
    MEMCHECK( data->table ) ;
    table->WriteUtf8((char *) data->table);
#endif
  }
  
  if (!type->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->type = (uint16_t *) malloc((type->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->type ) ;
    type->Write((uint16_t *) data->type);
#else
    data->type = (char *) malloc(type->Length() + 1);
    MEMCHECK( data->type ) ;
    type->WriteUtf8((char *) data->type);
#endif
  }
  
  data->conn = conn;
  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(), 
    work_req, 
    UV_Tables, 
    (uv_after_work_cb) UV_AfterQuery);

  conn->Ref();

  info.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Tables(uv_work_t* req) {
  query_work_data* data = (query_work_data *)(req->data);
  
  SQLAllocHandle(SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT );
  
  SQLRETURN ret = SQLTables( 
    data->hSTMT, 
    (SQLTCHAR *) data->catalog,   SQL_NTS, 
    (SQLTCHAR *) data->schema,   SQL_NTS, 
    (SQLTCHAR *) data->table,   SQL_NTS, 
    (SQLTCHAR *) data->type,   SQL_NTS
  );
  
  // this will be checked later in UV_AfterQuery
  data->result = ret; 
}



/*
 * Columns
 */

NAN_METHOD(ODBCConnection::Columns) {
  Nan::HandleScope scope;

  REQ_STRO_OR_NULL_ARG(0, catalog);
  REQ_STRO_OR_NULL_ARG(1, schema);
  REQ_STRO_OR_NULL_ARG(2, table);
  REQ_STRO_OR_NULL_ARG(3, column);
  
  Local<Function> cb = Local<Function>::Cast(info[4]);
  
  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));
  MEMCHECK( data ) ;

  data->sql = NULL;
  data->catalog = NULL;
  data->schema = NULL;
  data->table = NULL;
  data->type = NULL;
  data->column = NULL;
  data->cb = new Nan::Callback(cb);

  if (!catalog->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->catalog = (uint16_t *) malloc((catalog->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->catalog ) ;
    catalog->Write((uint16_t *) data->catalog);
#else
    data->catalog = (char *) malloc(catalog->Length() + 1);
    MEMCHECK( data->catalog ) ;
    catalog->WriteUtf8((char *) data->catalog);
#endif
  }

  if (!schema->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->schema = (uint16_t *) malloc((schema->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->schema ) ;
    schema->Write((uint16_t *) data->schema);
#else
    data->schema = (char *) malloc(schema->Length() + 1);
    MEMCHECK( data->schema ) ;
    schema->WriteUtf8((char *) data->schema);
#endif
  }
  
  if (!table->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->table = (uint16_t *) malloc((table->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->table ) ;
    table->Write((uint16_t *) data->table);
#else
    data->table = (char *) malloc(table->Length() + 1);
    MEMCHECK( data->table ) ;
    table->WriteUtf8((char *) data->table);
#endif
  }
  
  if (!column->Equals(Nan::New("null").ToLocalChecked())) {
#ifdef UNICODE
    data->column = (uint16_t *) malloc((column->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
    MEMCHECK( data->column ) ;
    column->Write((uint16_t *) data->column);
#else
    data->column = (char *) malloc(column->Length() + 1);
    MEMCHECK( data->column ) ;
    column->WriteUtf8((char *) data->column);
#endif
  }
  
  data->conn = conn;
  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(),
    work_req, 
    UV_Columns, 
    (uv_after_work_cb)UV_AfterQuery);
  
  conn->Ref();

  info.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Columns(uv_work_t* req) {
  query_work_data* data = (query_work_data *)(req->data);
  
  SQLAllocHandle(SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT );
  
  SQLRETURN ret = SQLColumns( 
    data->hSTMT, 
    (SQLTCHAR *) data->catalog,   SQL_NTS, 
    (SQLTCHAR *) data->schema,   SQL_NTS, 
    (SQLTCHAR *) data->table,   SQL_NTS, 
    (SQLTCHAR *) data->column,   SQL_NTS
  );
  
  // this will be checked later in UV_AfterQuery
  data->result = ret;
}

struct TransactionJob : public Job<TransactionJob, ODBCConnection> {
  enum Kind {
    BEGIN_TRANSACTION,
    COMMIT_TRANSACTION,
    ROLLBACK_TRANSACTION,
    SET_ISOLATION_LEVEL
  };

  inline explicit TransactionJob(Kind kind)
      : kind(kind), isolation_level() {}

  inline TransactionJob(Kind kind, int isolation_level)
      : kind(kind), isolation_level(isolation_level) {
    assert(kind == SET_ISOLATION_LEVEL);
  }

  inline void Work(ODBCConnection* conn) {
    if (kind == BEGIN_TRANSACTION) {
      result = SetAttribute(conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF);
      if (!SQL_SUCCEEDED(result)) RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
    } else if (kind == COMMIT_TRANSACTION || kind == ROLLBACK_TRANSACTION) {
      SQLSMALLINT completion_type =
          (kind == COMMIT_TRANSACTION ? SQL_COMMIT : SQL_ROLLBACK);
      result = SQLEndTran(SQL_HANDLE_DBC, conn->m_hDBC, completion_type);
      if (!SQL_SUCCEEDED(result)) RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
      SQLRETURN temp_result =
          SetAttribute(conn, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);
      if (SQL_SUCCEEDED(result) && !SQL_SUCCEEDED(temp_result)) {
        RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
        result = temp_result;
      }
    } else if (kind == SET_ISOLATION_LEVEL) {
      result = SetAttribute(conn, SQL_ATTR_TXN_ISOLATION, isolation_level);
      if (!SQL_SUCCEEDED(result)) RecordSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
    } else {
      UNREACHABLE();
    }
  }

  inline void Done(ODBCConnection* conn) {
    if (SQL_SUCCEEDED(result)) {
      MakeCallback();
    } else {
      MakeCallback(MakeSQLError());
    }
  }

  inline void DoneSync(ODBCConnection* conn,
                       const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (SQL_SUCCEEDED(result)) {
      info.GetReturnValue().Set(true);
    } else {
      Nan::ThrowError(MakeSQLError());
    }
  }

  inline SQLRETURN SetAttribute(ODBCConnection* conn, int option, int value) {
    SQLPOINTER value_p =
        reinterpret_cast<SQLPOINTER>(static_cast<uintptr_t>(value));
    return SQLSetConnectAttr(conn->m_hDBC, option, value_p, SQL_NO_DATA);
  }

  const Kind kind;
  const int isolation_level;
  SQLRETURN result;
};

NAN_METHOD(ODBCConnection::BeginTransaction) {
  static const int kCallbackIndex = 0;
  TransactionJob* that =
      new(std::nothrow) TransactionJob(TransactionJob::BEGIN_TRANSACTION);
  TransactionJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCConnection::BeginTransactionSync) {
  TransactionJob that(TransactionJob::BEGIN_TRANSACTION);
  TransactionJob::Sync(HERE, info, &that);
}

inline TransactionJob::Kind ToTransactionKind(v8::Local<v8::Value> value) {
  if (value->BooleanValue()) {
    return TransactionJob::ROLLBACK_TRANSACTION;
  }
  return TransactionJob::COMMIT_TRANSACTION;
}

NAN_METHOD(ODBCConnection::EndTransaction) {
  static const int kCallbackIndex = 1;
  const TransactionJob::Kind kind = ToTransactionKind(info[0]);
  TransactionJob* that = new(std::nothrow) TransactionJob(kind);
  TransactionJob::Async(HERE, info, that, kCallbackIndex);
}

NAN_METHOD(ODBCConnection::EndTransactionSync) {
  const TransactionJob::Kind kind = ToTransactionKind(info[0]);
  TransactionJob that(kind);
  TransactionJob::Sync(HERE, info, &that);
}

// TODO(bnoordhuis) Should SetIsolationLevel() have an async counterpart?
NAN_METHOD(ODBCConnection::SetIsolationLevel) {
  int isolation_level;
  if (info.Length() == 0) {
    isolation_level = SQL_TXN_READ_COMMITTED;
  } else if (info[0]->IsInt32()) {
    isolation_level = info[0]->Int32Value();
  } else {
    return Nan::ThrowTypeError("Argument 0 must be an integer");
  }
  TransactionJob that(TransactionJob::SET_ISOLATION_LEVEL, isolation_level);
  TransactionJob::Sync(HERE, info, &that);
}
