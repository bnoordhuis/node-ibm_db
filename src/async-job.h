// Copyright (c) 2017, IBM Corporation <opendev@us.ibm.com>
//
// Permission to use, copy, modify, and/or distribute this software for any
// purpose with or without fee is hereby granted, provided that the above
// copyright notice and this permission notice appear in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

#ifndef SRC_ASYNC_JOB_H_
#define SRC_ASYNC_JOB_H_

#include "nan.h"
#include "uv.h"
#include "v8.h"

#include "sql-util.h"
#include "util.h"

#include <assert.h>
#include <stdio.h>  // snprintf()

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)
#define HERE __FILE__ ":" STRINGIFY(__LINE__)

namespace {

template <class T, class U>
struct Job {
  inline Job() : sql_state(), sql_error() {}

  inline static void Async(const char where[],
                           const Nan::FunctionCallbackInfo<v8::Value>& info,
                           T* that, int cb_index) {
    v8::Local<v8::Value> cb_v = info[cb_index];
    if (!cb_v->IsFunction()) {
      char buffer[256];
      snprintf(buffer, sizeof(buffer), "Argument %d must be a function",
               cb_index);
      return Nan::ThrowTypeError(buffer);
    }
    if (that == NULL) {
      char buffer[256];
      snprintf(buffer, sizeof(buffer),
               "Could not allocate enough memory in ibm_db file %s.", where);
      Nan::LowMemoryNotification();
      return Nan::ThrowError(buffer);
    }
    that->holder.Reset(info.Holder());
    that->callback.SetFunction(cb_v.As<v8::Function>());
    that->handle = Nan::ObjectWrap::Unwrap<U>(info.Holder());
    uv_queue_work(uv_default_loop(), &that->work_req, Work, Done);
  }

  inline static void Sync(const char where[],
                          const Nan::FunctionCallbackInfo<v8::Value>& info,
                          T* that) {
    U* handle = Nan::ObjectWrap::Unwrap<U>(info.Holder());
    that->Work(handle);
    that->DoneSync(handle, info);
  }

  inline static void Work(uv_work_t* req) {
    T* that = ContainerOf(&T::work_req, req);
    that->Work(that->handle);
  }

  inline static void Done(uv_work_t* req, int status) {
    assert(status == 0);
    Nan::HandleScope handle_scope;
    T* that = ContainerOf(&T::work_req, req);
    that->Done(that->handle);
    delete that;
  }

  inline void MakeCallback(int argc, v8::Local<v8::Value> argv[]) {
    Nan::HandleScope handle_scope;
    callback.Call(argc, argv);
  }

  inline void MakeCallback(v8::Local<v8::Value> arg) {
    MakeCallback(1, &arg);
  }

  inline void MakeCallback() {
    MakeCallback(0, NULL);
  }

  inline void RecordSQLError(SQLSMALLINT handle_type, SQLHANDLE handle) {
    WriteSQLError(handle_type, handle, &sql_state, &sql_error);
  }

  inline v8::Local<v8::Value> MakeSQLError(const char* error_message = NULL) {
    if (error_message == NULL)
      error_message = "[node-odbc] SQL_ERROR";

    v8::Local<v8::String> message_string = Nan::New(sql_error).ToLocalChecked();
    v8::Local<v8::Value> exception_v = Nan::Error(message_string);

    if (exception_v->IsObject()) {
      v8::Local<v8::Object> exception = exception_v.As<v8::Object>();

      // XXX(bnoordhuis) For backwards compatibility.
      // ODBC::GetSQLError() leaves it empty, too.
      Nan::Set(exception, Nan::New("errors").ToLocalChecked(),
               Nan::New<v8::Array>());

      Nan::Set(exception, Nan::New("error").ToLocalChecked(),
               Nan::New(error_message).ToLocalChecked());

      Nan::Set(exception, Nan::New("state").ToLocalChecked(),
               Nan::New(sql_state).ToLocalChecked());
    }

    return exception_v;
  }

  uv_work_t work_req;
  Nan::Persistent<v8::Object> holder;
  Nan::Callback callback;
  U* handle;
  SQLTCHAR sql_state[kSqlStateSize];
  SQLTCHAR sql_error[kSqlErrorStringSize];

private:
  // Disallow copying.
  Job(const Job&);
  void operator=(const Job&);
};

}  // namespace anonymous

#endif  // SRC_ASYNC_JOB_H_
