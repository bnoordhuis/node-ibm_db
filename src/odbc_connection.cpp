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
  conn->connectTimeout = DEFAULT_CONNECTION_TIMEOUT ;
  
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

/*
 * Open
 * 
 */

//Handle<Value> ODBCConnection::Open(const Arguments& info) {
NAN_METHOD(ODBCConnection::Open) {
  DEBUG_PRINTF("ODBCConnection::Open\n");
  Nan::HandleScope scope;

  REQ_STRO_ARG(0, connection);
  REQ_FUN_ARG(1, cb);

  //get reference to the connection object
  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  //create a uv work request
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
 
  //allocate our worker data
  open_connection_work_data* data = (open_connection_work_data *) 
    calloc(1, sizeof(open_connection_work_data));
  MEMCHECK( data ) ;

  data->connectionLength = connection->Length() + 1;

  //copy the connection string to the work data  
#ifdef UNICODE
  data->connection = (uint16_t *) malloc(sizeof(uint16_t) * data->connectionLength);
  MEMCHECK( data->connection ) ;
  connection->Write((uint16_t*) data->connection);
#else
  data->connection = (char *) malloc(sizeof(char) * data->connectionLength);
  MEMCHECK( data->connection ) ;
  connection->WriteUtf8((char*) data->connection);
#endif
  
  data->cb = new Nan::Callback(cb);
  data->conn = conn;
  
  work_req->data = data;
  
  //queue the work
  uv_queue_work(uv_default_loop(), 
    work_req, 
    UV_Open, 
    (uv_after_work_cb)UV_AfterOpen);

  conn->Ref();

  info.GetReturnValue().Set(info.Holder());
}

void ODBCConnection::UV_Open(uv_work_t* req) {
  DEBUG_PRINTF("ODBCConnection::UV_Open\n");
  open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
  ODBCConnection* self = data->conn->self();

  SetConnectionTimeOut(self->m_hDBC, self->connectTimeout);
  
  //Attempt to connect
  //NOTE: SQLDriverConnect requires the thread to be locked
  int ret = SQLDriverConnect(
    self->m_hDBC,                   //ConnectionHandle
    NULL,                           //WindowHandle
    (SQLTCHAR*) data->connection,   //InConnectionString
    data->connectionLength,         //StringLength1
    NULL,                           //OutConnectionString
    0,                              //BufferLength - in characters
    NULL,                           //StringLength2Ptr
    SQL_DRIVER_NOPROMPT);           //DriverCompletion
  
  
  if (SQL_SUCCEEDED(ret)) {
    SQLHSTMT hStmt;
    
    //allocate a temporary statment
    ret = SQLAllocHandle(SQL_HANDLE_STMT, self->m_hDBC, &hStmt);
    
    //try to determine if the driver can handle
    //multiple recordsets
    ret = SQLGetFunctions(
      self->m_hDBC,
      SQL_API_SQLMORERESULTS, 
      &self->canHaveMoreResults);

    if (!SQL_SUCCEEDED(ret)) {
      self->canHaveMoreResults = 0;
    }
    
    //free the handle
    ret = SQLFreeHandle( SQL_HANDLE_STMT, hStmt);
    hStmt = (SQLHSTMT)NULL;
  }

  data->result = ret;
}

void ODBCConnection::UV_AfterOpen(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterOpen\n");
  Nan::HandleScope scope;
  
  open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
  Local<Value> argv[1];
  
  bool err = false;

  if (data->result) {
    err = true;

    Local<Value> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
    
    argv[0] = objError;
  }

  if (!err) {
   data->conn->self()->connected = true;
  }

  Nan::TryCatch try_catch;

  data->conn->Unref();
  data->cb->Call(err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  delete data->cb;
  
  free(data->connection);
  free(data);
  free(req);
}

void ODBCConnection::SetConnectionTimeOut( SQLHDBC hDBC, SQLUINTEGER timeOut )
{
    SQLRETURN rc = SQL_SUCCESS;
    DEBUG_PRINTF("ODBCConnection::SetConnectionTimeOut - timeOut = %i\n", timeOut);

    if(timeOut < 0 || timeOut > 32767)
    {
        timeOut = DEFAULT_CONNECTION_TIMEOUT ;
        DEBUG_PRINTF("ODBCConnection::SetConnectionTimeOut - Invalid connection timeout value changed to default.");
    }
    if (timeOut > 0)
    {
      //NOTE: SQLSetConnectAttr requires the thread to be locked
      rc = SQLSetConnectAttr( hDBC,           //ConnectionHandle
                              SQL_ATTR_LOGIN_TIMEOUT, //Attribute
                              (SQLPOINTER)(intptr_t)timeOut,    //ValuePtr
                              sizeof(timeOut));       //StringLength
    }
    if(rc != SQL_SUCCESS)
    {
        // We should not disallow connection if rc is not success, though it would never happen.
        // So, ignore any rc and just log here the value for debug build.
        DEBUG_PRINTF("ODBCConnection::SetConnectionTimeOut - rc = %i\n", rc);
    }
}

/*
 * OpenSync
 */

NAN_METHOD(ODBCConnection::OpenSync) {
  DEBUG_PRINTF("ODBCConnection::OpenSync\n");
  Nan::HandleScope scope;

  REQ_STRO_ARG(0, connection);

  //get reference to the connection object
  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
 
  Local<Value> objError;
  SQLRETURN ret;
  bool err = false;
  
  int connectionLength = connection->Length() + 1;
  
#ifdef UNICODE
  uint16_t* connectionString = (uint16_t *) malloc(connectionLength * sizeof(uint16_t));
  MEMCHECK( connectionString ) ;
  connection->Write(connectionString);
#else
  char* connectionString = (char *) malloc(connectionLength);
  MEMCHECK( connectionString ) ;
  connection->WriteUtf8(connectionString);
#endif
  
  SetConnectionTimeOut(conn->m_hDBC, conn->connectTimeout);
  
  //Attempt to connect
  //NOTE: SQLDriverConnect requires the thread to be locked
  ret = SQLDriverConnect(
    conn->m_hDBC,                   //ConnectionHandle
    NULL,                           //WindowHandle
    (SQLTCHAR*) connectionString,   //InConnectionString
    connectionLength,               //StringLength1
    NULL,                           //OutConnectionString
    0,                              //BufferLength - in characters
    NULL,                           //StringLength2Ptr
    SQL_DRIVER_NOPROMPT);           //DriverCompletion

  if (!SQL_SUCCEEDED(ret)) {
    err = true;
    
    objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->self()->m_hDBC);
  }
  else {
    SQLHSTMT hStmt;
    
    //allocate a temporary statment
    ret = SQLAllocHandle(SQL_HANDLE_STMT, conn->m_hDBC, &hStmt);
    
    //try to determine if the driver can handle
    //multiple recordsets
    ret = SQLGetFunctions(
      conn->m_hDBC,
      SQL_API_SQLMORERESULTS, 
      &conn->canHaveMoreResults);

    if (!SQL_SUCCEEDED(ret)) {
      conn->canHaveMoreResults = 0;
    }
  
    //free the handle
    ret = SQLFreeHandle( SQL_HANDLE_STMT, hStmt);
    hStmt = (SQLHSTMT)NULL;
    
    conn->self()->connected = true;
  }

  free(connectionString);
  
  if (err) {
    return Nan::ThrowError(objError);
  }
  else {
    info.GetReturnValue().Set(true);
  }
}

/*
 * Close
 * 
 */

NAN_METHOD(ODBCConnection::Close) {
  DEBUG_PRINTF("ODBCConnection::Close\n");
  Nan::HandleScope scope;

  REQ_FUN_ARG(0, cb);

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  close_connection_work_data* data = (close_connection_work_data *) 
    (calloc(1, sizeof(close_connection_work_data)));
  MEMCHECK( data ) ;

  data->cb = new Nan::Callback(cb);
  data->conn = conn;

  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(),
    work_req,
    UV_Close,
    (uv_after_work_cb)UV_AfterClose);

  conn->Ref();

  info.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Close(uv_work_t* req) {
  DEBUG_PRINTF("ODBCConnection::UV_Close\n");
  close_connection_work_data* data = (close_connection_work_data *)(req->data);
  ODBCConnection* conn = data->conn;
  
  //TODO: check to see if there are any open statements
  //on this connection
  
  conn->Free();
  conn->connected = false;
  data->result = 0;
}

void ODBCConnection::UV_AfterClose(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterClose\n");
  Nan::HandleScope scope;

  close_connection_work_data* data = (close_connection_work_data *)(req->data);

  ODBCConnection* conn = data->conn;
  
  Local<Value> argv[1];
  bool err = false;
  
  if (data->result) {
    err = true;
    argv[0] = Exception::Error(Nan::New("Error closing database").ToLocalChecked());
  }
  else {
    conn->connected = false;
  }

  Nan::TryCatch try_catch;

  data->conn->Unref();
  data->cb->Call(err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  delete data->cb;

  free(data);
  free(req);
}

/*
 * CloseSync
 */

NAN_METHOD(ODBCConnection::CloseSync) {
  DEBUG_PRINTF("ODBCConnection::CloseSync\n");
  Nan::HandleScope scope;

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  //TODO: check to see if there are any open statements
  //on this connection
  
  conn->Free();
  
  conn->connected = false;

  info.GetReturnValue().Set(true);
}

/*
 * CreateStatementSync
 * 
 */

NAN_METHOD(ODBCConnection::CreateStatementSync) {
  DEBUG_PRINTF("ODBCConnection::CreateStatementSync\n");
  Nan::HandleScope scope;

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
   
  SQLHSTMT hSTMT;

  SQLAllocHandle(
    SQL_HANDLE_STMT, 
    conn->m_hDBC, 
    &hSTMT);
  
  Local<Value> params[3];
  params[0] = Nan::New<External>((void*)(intptr_t)conn->m_hENV);
  params[1] = Nan::New<External>((void*)(intptr_t)conn->m_hDBC);
  params[2] = Nan::New<External>((void*)(intptr_t)hSTMT);
  
  Local<Object> js_result = NewInstance(ODBCStatement::constructor, 3, params);
  
  info.GetReturnValue().Set(js_result);
}

/*
 * CreateStatement
 * 
 */

NAN_METHOD(ODBCConnection::CreateStatement) {
  DEBUG_PRINTF("ODBCConnection::CreateStatement\n");
  Nan::HandleScope scope;

  REQ_FUN_ARG(0, cb);

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
    
  //initialize work request
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  //initialize our data
  create_statement_work_data* data = 
    (create_statement_work_data *) (calloc(1, sizeof(create_statement_work_data)));
  MEMCHECK( data ) ;

  data->cb = new Nan::Callback(cb);
  data->conn = conn;

  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(), 
    work_req, 
    UV_CreateStatement, 
    (uv_after_work_cb)UV_AfterCreateStatement);

  conn->Ref();

  info.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_CreateStatement(uv_work_t* req) {
  DEBUG_PRINTF("ODBCConnection::UV_CreateStatement\n");
  
  //get our work data
  create_statement_work_data* data = (create_statement_work_data *)(req->data);

  DEBUG_PRINTF("ODBCConnection::UV_CreateStatement m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",
    data->conn->m_hENV,
    data->conn->m_hDBC,
    data->hSTMT
  );
  
  //allocate a new statment handle
  SQLAllocHandle( SQL_HANDLE_STMT, 
                  data->conn->m_hDBC, 
                  &data->hSTMT);

  DEBUG_PRINTF("ODBCConnection::UV_CreateStatement m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",
    data->conn->m_hENV,
    data->conn->m_hDBC,
    data->hSTMT
  );
}

void ODBCConnection::UV_AfterCreateStatement(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterCreateStatement\n");
  Nan::HandleScope scope;

  create_statement_work_data* data = (create_statement_work_data *)(req->data);

  DEBUG_PRINTF("ODBCConnection::UV_AfterCreateStatement m_hDBC=%X m_hDBC=%X hSTMT=%X\n",
    data->conn->m_hENV,
    data->conn->m_hDBC,
    data->hSTMT
  );
  
  Local<Value> info[3];
  info[0] = Nan::New<External>((void*)(intptr_t)data->conn->m_hENV);
  info[1] = Nan::New<External>((void*)(intptr_t)data->conn->m_hDBC);
  info[2] = Nan::New<External>((void*)(intptr_t)data->hSTMT);
  
  Local<Object> js_result = NewInstance(ODBCStatement::constructor, 3, info);

  info[0] = Nan::Null();
  info[1] = js_result;


  Nan::TryCatch try_catch;

  data->cb->Call( 2, info);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }
  
  data->conn->Unref();
  delete data->cb;

  free(data);
  free(req);
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
    bool* canFreeHandle = new bool(true);
    
    info[0] = Nan::New<External>((void*)(intptr_t)data->conn->m_hENV);
    info[1] = Nan::New<External>((void*)(intptr_t)data->conn->m_hDBC);
    info[2] = Nan::New<External>((void*)(intptr_t)data->hSTMT);
    info[3] = Nan::New<External>((void*)canFreeHandle);
    
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
    bool* canFreeHandle = new bool(true);
    
    result[0] = Nan::New<External>((void*) (intptr_t) conn->m_hENV);
    result[1] = Nan::New<External>((void*) (intptr_t) conn->m_hDBC);
    result[2] = Nan::New<External>((void*) (intptr_t) hSTMT);
    result[3] = Nan::New<External>((void*)canFreeHandle);
    
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

/*
 * BeginTransactionSync
 * 
 */

NAN_METHOD(ODBCConnection::BeginTransactionSync) {
  DEBUG_PRINTF("ODBCConnection::BeginTransactionSync\n");
  Nan::HandleScope scope;

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  SQLRETURN ret;

  //set the connection manual commits
  ret = SQLSetConnectAttr(
    conn->m_hDBC,
    SQL_ATTR_AUTOCOMMIT,
    (SQLPOINTER) SQL_AUTOCOMMIT_OFF,
    SQL_NTS);
  
  if (!SQL_SUCCEEDED(ret)) {
    Local<Value> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
    
    Nan::ThrowError(objError);
    
    info.GetReturnValue().Set(false);
  }
  
  info.GetReturnValue().Set(true);
}

/*
 * BeginTransaction
 * 
 */

NAN_METHOD(ODBCConnection::BeginTransaction) {
  DEBUG_PRINTF("ODBCConnection::BeginTransaction\n");
  Nan::HandleScope scope;

  REQ_FUN_ARG(0, cb);

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  query_work_data* data = 
    (query_work_data *) calloc(1, sizeof(query_work_data));
  MEMCHECK( data ) ;
  
  data->cb = new Nan::Callback(cb);
  data->conn = conn;
  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(),
    work_req, 
    UV_BeginTransaction, 
    (uv_after_work_cb)UV_AfterBeginTransaction);

  return;
}

/*
 * UV_BeginTransaction
 * 
 */

void ODBCConnection::UV_BeginTransaction(uv_work_t* req) {
  DEBUG_PRINTF("ODBCConnection::UV_BeginTransaction\n");
  
  query_work_data* data = (query_work_data *)(req->data);
  
  //set the connection manual commits
  data->result = SQLSetConnectAttr(
    data->conn->self()->m_hDBC,
    SQL_ATTR_AUTOCOMMIT,
    (SQLPOINTER) SQL_AUTOCOMMIT_OFF,
    SQL_NTS);
}

/*
 * UV_AfterBeginTransaction
 * 
 */

void ODBCConnection::UV_AfterBeginTransaction(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterBeginTransaction\n");
  Nan::HandleScope scope;

  //TODO: Is this supposed to be of type query_work_data?
  open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
  Local<Value> argv[1];
  
  bool err = false;

  if (!SQL_SUCCEEDED(data->result)) {
    err = true;

    Local<Value> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
    
    argv[0] = objError;
  }

  Nan::TryCatch try_catch;

  data->cb->Call( err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  delete data->cb;
  
  free(data);
  free(req);
}

/*
 * EndTransactionSync
 * 
 */

NAN_METHOD(ODBCConnection::EndTransactionSync) {
  DEBUG_PRINTF("ODBCConnection::EndTransactionSync\n");
  Nan::HandleScope scope;

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  REQ_BOOL_ARG(0, rollback);
  
  Local<Value> objError;
  SQLRETURN ret;
  bool error = false;
  SQLSMALLINT completionType = (rollback->Value()) 
    ? SQL_ROLLBACK
    : SQL_COMMIT
    ;
  
  //Call SQLEndTran
  ret = SQLEndTran(
    SQL_HANDLE_DBC,
    conn->m_hDBC,
    completionType);
  
  //check how the transaction went
  if (!SQL_SUCCEEDED(ret)) {
    error = true;
    
    objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
  }
  
  //Reset the connection back to autocommit
  ret = SQLSetConnectAttr(
    conn->m_hDBC,
    SQL_ATTR_AUTOCOMMIT,
    (SQLPOINTER) SQL_AUTOCOMMIT_ON,
    SQL_NTS);
  
  //check how setting the connection attr went
  //but only process the code if an error has not already
  //occurred. If an error occurred during SQLEndTran,
  //that is the error that we want to throw.
  if (!SQL_SUCCEEDED(ret) && !error) {
    //TODO: if this also failed, we really should
    //be restarting the connection or something to deal with this state
    error = true;
    
    objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
  }
  
  if (error) {
    Nan::ThrowError(objError);
    
    info.GetReturnValue().Set(false);
  }
  else {
    info.GetReturnValue().Set(true);
  }
}

/*
 * EndTransaction
 * 
 */

NAN_METHOD(ODBCConnection::EndTransaction) {
  DEBUG_PRINTF("ODBCConnection::EndTransaction\n");
  Nan::HandleScope scope;

  REQ_BOOL_ARG(0, rollback);
  REQ_FUN_ARG(1, cb);

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
  uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  MEMCHECK( work_req ) ;
  
  query_work_data* data = 
    (query_work_data *) calloc(1, sizeof(query_work_data));
  MEMCHECK( data ) ;
  
  data->completionType = (rollback->Value()) 
    ? SQL_ROLLBACK
    : SQL_COMMIT
    ;
  data->cb = new Nan::Callback(cb);
  data->conn = conn;
  work_req->data = data;
  
  uv_queue_work(
    uv_default_loop(),
    work_req, 
    UV_EndTransaction, 
    (uv_after_work_cb)UV_AfterEndTransaction);

  info.GetReturnValue().SetUndefined();
}

/*
 * UV_EndTransaction
 * 
 */

void ODBCConnection::UV_EndTransaction(uv_work_t* req) {
  DEBUG_PRINTF("ODBCConnection::UV_EndTransaction\n");
  
  query_work_data* data = (query_work_data *)(req->data);
  
  bool err = false;
  
  //Call SQLEndTran
  SQLRETURN ret = SQLEndTran(
    SQL_HANDLE_DBC,
    data->conn->m_hDBC,
    data->completionType);
  
  data->result = ret;
  
  if (!SQL_SUCCEEDED(ret)) {
    err = true;
  }
  
  //Reset the connection back to autocommit
  ret = SQLSetConnectAttr(
    data->conn->m_hDBC,
    SQL_ATTR_AUTOCOMMIT,
    (SQLPOINTER) SQL_AUTOCOMMIT_ON,
    SQL_NTS);
  
  if (!SQL_SUCCEEDED(ret) && !err) {
    //there was not an earlier error,
    //so we shall pass the return code from
    //this last call.
    data->result = ret;
  }
}

/*
 * UV_AfterEndTransaction
 * 
 */

void ODBCConnection::UV_AfterEndTransaction(uv_work_t* req, int status) {
  DEBUG_PRINTF("ODBCConnection::UV_AfterEndTransaction\n");
  Nan::HandleScope scope;
  
  open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
  Local<Value> argv[1];
  
  bool err = false;

  if (!SQL_SUCCEEDED(data->result)) {
    err = true;

    Local<Value> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
    
    argv[0] = objError;
  }

  Nan::TryCatch try_catch;

  data->cb->Call(err ? 1 : 0, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
  }

  delete data->cb;
  
  free(data);
  free(req);
}

/*
 * SetIsolationLevel
 * 
 */

NAN_METHOD(ODBCConnection::SetIsolationLevel) {
  DEBUG_PRINTF("ODBCConnection::SetIsolationLevel\n");
  Nan::HandleScope scope;

  ODBCConnection* conn = Nan::ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

  OPT_INT_ARG(0, isolationLevel, SQL_TXN_READ_COMMITTED)

  Local<Value> objError;
  SQLRETURN ret;
  bool error = false;

  //set the connection manual commits
  ret = SQLSetConnectAttr(
    conn->m_hDBC,
    SQL_ATTR_TXN_ISOLATION,
    (SQLPOINTER) isolationLevel,
    SQL_NTS);

  DEBUG_PRINTF("ODBCConnection::SetIsolationLevel isolationLevel=%i; ret=%d\n",
               isolationLevel, ret);

  //check how the transaction went
  if (!SQL_SUCCEEDED(ret)) {
    error = true;

    objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
  }

  if (error) {
    Nan::ThrowError(objError);

    info.GetReturnValue().Set(false);
  }
  else {
    info.GetReturnValue().Set(true);
  }
}
