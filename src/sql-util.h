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

#ifndef SRC_SQL_UTIL_H_
#define SRC_SQL_UTIL_H_

#include "util.h"

#include <stdio.h>    // snprintf()
#include <stddef.h>   // size_t
#include <sqlcli.h>

namespace {

// According to the IBM Knowledge Center, the SQL_DIAG_SQLSTATE is always
// exactly five characters long but it doesn't specify whether drivers
// can emit a trailing nul byte.  I'm not taking chances.
static const int kSqlStateSize = 6;

// Add room for a trailing nul byte.
static const int kSqlErrorStringSize = 1 + SQL_MAX_MESSAGE_LENGTH;

template <size_t N>
inline void CopyString(const char* message, SQLTCHAR (*buffer)[N]) {
  size_t index;

  for (index = 0; index < N && message[index] != '\0'; index += 1) {
    (*buffer)[index] = static_cast<SQLTCHAR>(message[index]);
  }

  if (index == N) {
    (*buffer)[N - 1] = static_cast<SQLTCHAR>(0);
  }
}

inline void WriteSQLError(SQLSMALLINT handle_type, SQLHANDLE handle,
                          SQLTCHAR (*sql_state)[kSqlStateSize],
                          SQLTCHAR (*buffer)[kSqlErrorStringSize]) {
  char errbuf[64];

  const char dummy_sql_state[kSqlStateSize] = "XXXXX";
  CopyString(dummy_sql_state, sql_state);

  // XXX(bnoordhuis) Note that we only capture the last diagnostic record.
  // This code replaces ODBC::GetSQLError() which ostensibly captured them
  // all but due to a bug also only logged the last one.  I speculate that
  // most errors emit a single diagnostic, making it an academic issue.
  SQLINTEGER last_record;
  SQLRETURN result =
      SQLGetDiagField(handle_type, handle, 0, SQL_DIAG_NUMBER,
                      &last_record, 0, NULL);
  if (!SQL_SUCCEEDED(result)) {
    snprintf(errbuf, sizeof(errbuf), "SQLGetDiagField(): error %d\n", result);
    CopyString(errbuf, buffer);
    return;
  }

  SQLSMALLINT size;
  result =
      SQLGetDiagRec(handle_type, handle, last_record, *sql_state,
                    NULL, *buffer, arraysize(*buffer) - 1, &size);
  if (!SQL_SUCCEEDED(result)) {
    snprintf(errbuf, sizeof(errbuf), "SQLGetDiagRec(): error %d\n", result);
    CopyString(errbuf, buffer);
    return;
  }

  (*buffer)[size] = static_cast<SQLTCHAR>(0);
}

}  // namespace anonymous

#endif  // SRC_SQL_UTIL_H_
