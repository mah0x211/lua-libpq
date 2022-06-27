/**
 *  Copyright (C) 2022 Masatoshi Fukunaga
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"),
 *  to deal in the Software without restriction, including without limitation
 *  the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 *  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

// lua
#include "lua_libpq.h"

// /* Support for overriding sslpassword handling with a callback */
// typedef int (*PQsslKeyPassHook_OpenSSL_type) (char *buf, int size, PGconn
// *conn); extern PQsslKeyPassHook_OpenSSL_type
// PQgetSSLKeyPassHook_OpenSSL(void); extern void
// PQsetSSLKeyPassHook_OpenSSL(PQsslKeyPassHook_OpenSSL_type hook); extern int
// PQdefaultSSLKeyPassHook_OpenSSL(char *buf, int size, PGconn *conn);

static int valid_server_encoding_id_lua(lua_State *L)
{
    int encoding = lauxh_checkinteger(L, 1);

    lua_pushboolean(L, pg_valid_server_encoding_id(encoding));
    return 1;
}

static int encoding_to_char_lua(lua_State *L)
{
    int encoding = lauxh_checkinteger(L, 1);

    lua_pushstring(L, pg_encoding_to_char(encoding));
    return 1;
}

static int char_to_encoding_lua(lua_State *L)
{
    const char *name = lauxh_checkstring(L, 1);

    lua_pushinteger(L, pg_char_to_encoding(name));
    return 1;
}

static int encrypt_password_lua(lua_State *L)
{
    const char *passwd = lauxh_checkstring(L, 1);
    const char *user   = lauxh_checkstring(L, 2);
    char *res          = PQencryptPassword(passwd, user);

    if (res) {
        lua_settop(L, 0);
        lua_pushstring(L, res);
        PQfreemem(res);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "encrypt_password");
    return 2;
}

static int env2encoding_lua(lua_State *L)
{
    // Get encoding id from environment variable PGCLIENTENCODING
    lua_pushinteger(L, PQenv2encoding());
    return 1;
}

static int dsplen_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = lauxh_checkinteger(L, 2);

    // Determine display length of multibyte encoded char at *s
    lua_pushinteger(L, PQdsplen(s, encoding));
    return 1;
}

static int mblen_bounded_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = lauxh_checkinteger(L, 2);

    // Same, but not more than the distance to the end of string s
    lua_pushinteger(L, PQmblenBounded(s, encoding));
    return 1;
}

static int mblen_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = lauxh_checkinteger(L, 2);

    // Determine length of multibyte encoded char at *s
    lua_pushinteger(L, PQmblen(s, encoding));
    return 1;
}

static int lib_version_lua(lua_State *L)
{
    lua_pushinteger(L, PQlibVersion());
    return 1;
}

/* Quoting strings before inclusion in queries. */

/* These forms are deprecated! */
// extern size_t PQescapeString(char *to, const char *from, size_t length);
// extern unsigned char *PQescapeBytea(const unsigned char *from,
//                                     size_t from_length, size_t *to_length);

static int unescape_bytea_lua(lua_State *L)
{
    const char *strtext = lauxh_checkstring(L, 1);
    size_t len          = 0;
    unsigned char *to   = PQunescapeBytea((const unsigned char *)strtext, &len);

    if (to) {
        lua_settop(L, 0);
        lua_pushlstring(L, (const char *)to, len);
        PQfreemem((void *)to);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "unescape_bytea");
    return 2;
}

static int is_threadsafe_lua(lua_State *L)
{
    lua_pushboolean(L, PQisthreadsafe());
    return 1;
}

LUALIB_API int luaopen_libpq(lua_State *L)
{
    lua_errno_loadlib(L);

    // create module table
    lua_newtable(L);
    lauxh_pushfn2tbl(L, "is_threadsafe", is_threadsafe_lua);
    lauxh_pushfn2tbl(L, "unescape_bytea", unescape_bytea_lua);
    lauxh_pushfn2tbl(L, "lib_version", lib_version_lua);
    lauxh_pushfn2tbl(L, "mblen", mblen_lua);
    lauxh_pushfn2tbl(L, "mblen_bounded", mblen_bounded_lua);
    lauxh_pushfn2tbl(L, "dsplen", dsplen_lua);
    lauxh_pushfn2tbl(L, "env2encoding", env2encoding_lua);
    lauxh_pushfn2tbl(L, "encrypt_password", encrypt_password_lua);
    lauxh_pushfn2tbl(L, "char_to_encoding", char_to_encoding_lua);
    lauxh_pushfn2tbl(L, "encoding_to_char", encoding_to_char_lua);
    lauxh_pushfn2tbl(L, "valid_server_encoding_id",
                     valid_server_encoding_id_lua);
    // export libpq_conn
    libpq_conn_init(L);
    libpq_cancel_init(L);
    libpq_result_init(L);
    libpq_notify_init(L);
    libpq_util_init(L);

    //
    // Option flags for PQcopyResult
    //
    lauxh_pushint2tbl(L, "PG_COPYRES_ATTRS", PG_COPYRES_ATTRS);
    // Implies PG_COPYRES_ATTRS
    lauxh_pushint2tbl(L, "PG_COPYRES_TUPLES", PG_COPYRES_TUPLES);
    lauxh_pushint2tbl(L, "PG_COPYRES_EVENTS", PG_COPYRES_EVENTS);
    lauxh_pushint2tbl(L, "PG_COPYRES_NOTICEHOOKS", PG_COPYRES_NOTICEHOOKS);

    // ConnStatusType
    lauxh_pushint2tbl(L, "CONNECTION_OK", CONNECTION_OK);
    lauxh_pushint2tbl(L, "CONNECTION_BAD", CONNECTION_BAD);
    /* Non-blocking mode only below here */
    /*
     * The existence of these should never be relied upon - they should only
     * be used for user feedback or similar purposes.
     */
    // Waiting for connection to be made.
    lauxh_pushint2tbl(L, "CONNECTION_STARTED", CONNECTION_STARTED);
    // Connection OK; waiting to send.
    lauxh_pushint2tbl(L, "CONNECTION_MADE", CONNECTION_MADE);
    // Waiting for a response from the postmaster.
    lauxh_pushint2tbl(L, "CONNECTION_AWAITING_RESPONSE",
                      CONNECTION_AWAITING_RESPONSE);
    // Received authentication; waiting for backend startup.
    lauxh_pushint2tbl(L, "CONNECTION_AUTH_OK", CONNECTION_AUTH_OK);
    // This state is no longer used.
    lauxh_pushint2tbl(L, "CONNECTION_SETENV", CONNECTION_SETENV);
    // Negotiating SSL.
    lauxh_pushint2tbl(L, "CONNECTION_SSL_STARTUP", CONNECTION_SSL_STARTUP);
    // Internal state: connect() needed
    lauxh_pushint2tbl(L, "CONNECTION_NEEDED", CONNECTION_NEEDED);
    // Checking if session is read-write.
    lauxh_pushint2tbl(L, "CONNECTION_CHECK_WRITABLE",
                      CONNECTION_CHECK_WRITABLE);
    // Consuming any extra messages.
    lauxh_pushint2tbl(L, "CONNECTION_CONSUME", CONNECTION_CONSUME);
    // Negotiating GSSAPI.
    lauxh_pushint2tbl(L, "CONNECTION_GSS_STARTUP", CONNECTION_GSS_STARTUP);
    // Checking target server properties.
    lauxh_pushint2tbl(L, "CONNECTION_CHECK_TARGET", CONNECTION_CHECK_TARGET);
    // Checking if server is in standby mode.
    lauxh_pushint2tbl(L, "CONNECTION_CHECK_STANDBY", CONNECTION_CHECK_STANDBY);

    // PostgresPollingStatusType;
    lauxh_pushint2tbl(L, "PGRES_POLLING_FAILED", PGRES_POLLING_FAILED);
    // These two indicate that one may
    lauxh_pushint2tbl(L, "PGRES_POLLING_READING", PGRES_POLLING_READING);
    // use select before polling again.
    lauxh_pushint2tbl(L, "PGRES_POLLING_WRITING", PGRES_POLLING_WRITING);
    lauxh_pushint2tbl(L, "PGRES_POLLING_OK", PGRES_POLLING_OK);
    // unused; keep for awhile for backwards compatibility
    lauxh_pushint2tbl(L, "PGRES_POLLING_ACTIVE", PGRES_POLLING_ACTIVE);

    // ExecStatusType;
    /* empty query string was executed */
    lauxh_pushint2tbl(L, "PGRES_EMPTY_QUERY", PGRES_EMPTY_QUERY);
    // a query command that doesn't return
    // anything was executed properly by the
    // backend
    lauxh_pushint2tbl(L, "PGRES_COMMAND_OK", PGRES_COMMAND_OK);
    // a query command that returns tuples was
    // executed properly by the backend, PGresult
    // contains the result tuples
    lauxh_pushint2tbl(L, "PGRES_TUPLES_OK", PGRES_TUPLES_OK);
    // Copy Out data transfer in progress
    lauxh_pushint2tbl(L, "PGRES_COPY_OUT", PGRES_COPY_OUT);
    // Copy In data transfer in progress
    lauxh_pushint2tbl(L, "PGRES_COPY_IN", PGRES_COPY_IN);
    // an unexpected response was recv'd from the backend
    lauxh_pushint2tbl(L, "PGRES_BAD_RESPONSE", PGRES_BAD_RESPONSE);
    // notice or warning message
    lauxh_pushint2tbl(L, "PGRES_NONFATAL_ERROR", PGRES_NONFATAL_ERROR);
    // query failed
    lauxh_pushint2tbl(L, "PGRES_FATAL_ERROR", PGRES_FATAL_ERROR);
    // Copy In/Out data transfer in progress
    lauxh_pushint2tbl(L, "PGRES_COPY_BOTH", PGRES_COPY_BOTH);
    // single tuple from larger resultset
    lauxh_pushint2tbl(L, "PGRES_SINGLE_TUPLE", PGRES_SINGLE_TUPLE);
    // pipeline synchronization point
    lauxh_pushint2tbl(L, "PGRES_PIPELINE_SYNC", PGRES_PIPELINE_SYNC);
    // Command didn't run because of an abort earlier in a pipeline
    lauxh_pushint2tbl(L, "PGRES_PIPELINE_ABORTED", PGRES_PIPELINE_ABORTED);

    // PGTransactionStatusType
    // connection idle
    lauxh_pushint2tbl(L, "PQTRANS_IDLE", PQTRANS_IDLE);
    // command in progress
    lauxh_pushint2tbl(L, "PQTRANS_ACTIVE", PQTRANS_ACTIVE);
    // idle, within transaction block
    lauxh_pushint2tbl(L, "PQTRANS_INTRANS", PQTRANS_INTRANS);
    // idle, within failed transaction
    lauxh_pushint2tbl(L, "PQTRANS_INERROR", PQTRANS_INERROR);
    // cannot determine status
    lauxh_pushint2tbl(L, "PQTRANS_UNKNOWN", PQTRANS_UNKNOWN);

    // PGVerbosity
    // single-line error messages
    lauxh_pushint2tbl(L, "PQERRORS_TERSE", PQERRORS_TERSE);
    // recommended style
    lauxh_pushint2tbl(L, "PQERRORS_DEFAULT", PQERRORS_DEFAULT);
    // all the facts, ma'am
    lauxh_pushint2tbl(L, "PQERRORS_VERBOSE", PQERRORS_VERBOSE);
    // only error severity and SQLSTATE code
    lauxh_pushint2tbl(L, "PQERRORS_SQLSTATE", PQERRORS_SQLSTATE);

    // PGContextVisibility
    // never show CONTEXT field
    lauxh_pushint2tbl(L, "PQSHOW_CONTEXT_NEVER", PQSHOW_CONTEXT_NEVER);
    // show CONTEXT for errors only (default)
    lauxh_pushint2tbl(L, "PQSHOW_CONTEXT_ERRORS", PQSHOW_CONTEXT_ERRORS);
    // always show CONTEXT field
    lauxh_pushint2tbl(L, "PQSHOW_CONTEXT_ALWAYS", PQSHOW_CONTEXT_ALWAYS);

    // PGPing
    // server is accepting connections
    lauxh_pushint2tbl(L, "PQPING_OK", PQPING_OK);
    // server is alive but rejecting connections
    lauxh_pushint2tbl(L, "PQPING_REJECT", PQPING_REJECT);
    // could not establish connection
    lauxh_pushint2tbl(L, "PQPING_NO_RESPONSE", PQPING_NO_RESPONSE);
    // connection not attempted (bad params)
    lauxh_pushint2tbl(L, "PQPING_NO_ATTEMPT", PQPING_NO_ATTEMPT);

    /*
     * PGpipelineStatus - Current status of pipeline mode
     */
    // PGpipelineStatus;
    lauxh_pushint2tbl(L, "PQ_PIPELINE_OFF", PQ_PIPELINE_OFF);
    lauxh_pushint2tbl(L, "PQ_PIPELINE_ON", PQ_PIPELINE_ON);
    lauxh_pushint2tbl(L, "PQ_PIPELINE_ABORTED", PQ_PIPELINE_ABORTED);

    // flags controlling trace output:
    // omit timestamps from each line
    lauxh_pushint2tbl(L, "PQTRACE_SUPPRESS_TIMESTAMPS",
                      PQTRACE_SUPPRESS_TIMESTAMPS);
    // redact portions of some messages, for testing frameworks
    lauxh_pushint2tbl(L, "PQTRACE_REGRESS_MODE", PQTRACE_REGRESS_MODE);

    // Interface for multiple-result or asynchronous queries
    lauxh_pushint2tbl(L, "PQ_QUERY_PARAM_MAX_LIMIT", PQ_QUERY_PARAM_MAX_LIMIT);

    //
    // Identifiers of error message fields.  Kept here to keep common
    // between frontend and backend, and also to export them to libpq
    // applications.
    //
    lauxh_pushint2tbl(L, "PG_DIAG_SEVERITY", PG_DIAG_SEVERITY);
    lauxh_pushint2tbl(L, "PG_DIAG_SEVERITY_NONLOCALIZED",
                      PG_DIAG_SEVERITY_NONLOCALIZED);
    lauxh_pushint2tbl(L, "PG_DIAG_SQLSTATE", PG_DIAG_SQLSTATE);
    lauxh_pushint2tbl(L, "PG_DIAG_MESSAGE_PRIMARY", PG_DIAG_MESSAGE_PRIMARY);
    lauxh_pushint2tbl(L, "PG_DIAG_MESSAGE_DETAIL", PG_DIAG_MESSAGE_DETAIL);
    lauxh_pushint2tbl(L, "PG_DIAG_MESSAGE_HINT", PG_DIAG_MESSAGE_HINT);
    lauxh_pushint2tbl(L, "PG_DIAG_STATEMENT_POSITION",
                      PG_DIAG_STATEMENT_POSITION);
    lauxh_pushint2tbl(L, "PG_DIAG_INTERNAL_POSITION",
                      PG_DIAG_INTERNAL_POSITION);
    lauxh_pushint2tbl(L, "PG_DIAG_INTERNAL_QUERY", PG_DIAG_INTERNAL_QUERY);
    lauxh_pushint2tbl(L, "PG_DIAG_CONTEXT", PG_DIAG_CONTEXT);
    lauxh_pushint2tbl(L, "PG_DIAG_SCHEMA_NAME", PG_DIAG_SCHEMA_NAME);
    lauxh_pushint2tbl(L, "PG_DIAG_TABLE_NAME", PG_DIAG_TABLE_NAME);
    lauxh_pushint2tbl(L, "PG_DIAG_COLUMN_NAME", PG_DIAG_COLUMN_NAME);
    lauxh_pushint2tbl(L, "PG_DIAG_DATATYPE_NAME", PG_DIAG_DATATYPE_NAME);
    lauxh_pushint2tbl(L, "PG_DIAG_CONSTRAINT_NAME", PG_DIAG_CONSTRAINT_NAME);
    lauxh_pushint2tbl(L, "PG_DIAG_SOURCE_FILE", PG_DIAG_SOURCE_FILE);
    lauxh_pushint2tbl(L, "PG_DIAG_SOURCE_LINE", PG_DIAG_SOURCE_LINE);
    lauxh_pushint2tbl(L, "PG_DIAG_SOURCE_FUNCTION", PG_DIAG_SOURCE_FUNCTION);
    return 1;
}
