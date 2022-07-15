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

typedef struct {
    lua_State *L;
    int notice_proc_ref;
    int notice_recv_ref;
    int trace_ref;
    PQnoticeProcessor default_proc;
    PQnoticeReceiver default_recv;
    PGconn *conn;
} conn_t;

static inline conn_t *checkself(lua_State *L)
{
    conn_t *c = luaL_checkudata(L, 1, LIBPQ_CONN_MT);
    if (!c->conn) {
        luaL_error(L, "attempt to use a freed object");
    }
    return c;
}

PGconn *libpq_check_conn(lua_State *L)
{
    conn_t *c = checkself(L);
    return c->conn;
}

static int encrypt_password_conn_lua(lua_State *L)
{
    PGconn *conn          = libpq_check_conn(L);
    const char *passwd    = lauxh_checkstring(L, 2);
    const char *user      = lauxh_checkstring(L, 3);
    const char *algorithm = lauxh_optstring(L, 4, NULL);
    char *res = PQencryptPasswordConn(conn, passwd, user, algorithm);

    if (res) {
        lua_pushstring(L, res);
        PQfreemem(res);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int escape_bytea_conn_lua(lua_State *L)
{
    PGconn *conn     = libpq_check_conn(L);
    size_t len       = 0;
    const char *from = lauxh_checklstring(L, 2, &len);
    unsigned char *to =
        PQescapeByteaConn(conn, (const unsigned char *)from, len, &len);

    if (to) {
        lua_pushlstring(L, (const char *)to, len);
        PQfreemem((void *)to);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int escape_identifier_lua(lua_State *L)
{
    PGconn *conn    = libpq_check_conn(L);
    size_t len      = 0;
    const char *str = lauxh_checklstring(L, 2, &len);
    char *to        = PQescapeIdentifier(conn, str, len);

    if (to) {
        lua_pushstring(L, to);
        PQfreemem(to);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int escape_literal_lua(lua_State *L)
{
    PGconn *conn    = libpq_check_conn(L);
    size_t len      = 0;
    const char *str = lauxh_checklstring(L, 2, &len);
    char *to        = PQescapeLiteral(conn, str, len);

    if (to) {
        lua_pushstring(L, to);
        PQfreemem(to);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int escape_string_conn_lua(lua_State *L)
{
    PGconn *conn     = libpq_check_conn(L);
    size_t len       = 0;
    const char *from = lauxh_checklstring(L, 2, &len);
    // For safety the buffer at "to" must be at least 2*length + 1 bytes long.
    // A terminating NUL character is added to the output string, whether the
    // input is NUL-terminated or not.
    int err          = 0;
    char *to         = lua_newuserdata(L, len * 2 + 1);
    size_t to_len    = PQescapeStringConn(conn, to, from, len, &err);

    if (err) {
        lua_pushnil(L);
        lua_pushstring(L, PQerrorMessage(conn));
        return 2;
    }

    lua_pushlstring(L, to, to_len);
    return 1;
}

/* Create and manipulate PGresults */
static int make_empty_result_lua(lua_State *L)
{
    PGconn *conn   = libpq_check_conn(L);
    int status     = lauxh_optinteger(L, 2, PGRES_COMMAND_OK);
    PGresult **res = libpq_result_new(L, 1, 0);

    *res = PQmakeEmptyPGresult(conn, status);
    if (*res) {
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

/* Describe prepared statements and portals */
// extern PGresult *PQdescribePrepared(PGconn *conn, const char *stmt);
// extern PGresult *PQdescribePortal(PGconn *conn, const char *portal);
// extern int	PQsendDescribePrepared(PGconn *conn, const char *stmt);
// extern int	PQsendDescribePortal(PGconn *conn, const char *portal);

static int flush_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    switch (PQflush(conn)) {
    case 0:
        // done
        lua_pushboolean(L, 1);
        return 1;

    case 1:
        // should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        lua_pushboolean(L, 0);
        lua_pushstring(L, PQerrorMessage(conn));
        return 2;
    }
}

static int is_nonblocking_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushboolean(L, PQisnonblocking(conn));
    return 1;
}

static int set_nonblocking_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    int enabled  = lauxh_checkboolean(L, 2);

    if (PQsetnonblocking(conn, enabled) != -1) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

/* Deprecated routines for copy in/out */
// extern int	PQgetline(PGconn *conn, char *string, int length);
// extern int	PQputline(PGconn *conn, const char *string);
// extern int	PQgetlineAsync(PGconn *conn, char *buffer, int bufsize);
// extern int	PQputnbytes(PGconn *conn, const char *buffer, int nbytes);
// extern int	PQendcopy(PGconn *conn);

static int get_copy_data_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    int async    = lauxh_optboolean(L, 2, 0);
    char *buffer = NULL;
    int nbytes   = PQgetCopyData(conn, &buffer, async);

    switch (nbytes) {
    case -2:
        lua_pushnil(L);
        lua_pushstring(L, PQerrorMessage(conn));
        return 2;

    case -1:
        // completed
        return 0;

    case 0:
        // in-progress
        lua_pushnil(L);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        lua_pushlstring(L, buffer, nbytes);
        PQfreemem(buffer);
        return 1;
    }
}

static int put_copy_end_lua(lua_State *L)
{
    PGconn *conn         = libpq_check_conn(L);
    const char *errormsg = lauxh_optstring(L, 2, NULL);

    switch (PQputCopyEnd(conn, errormsg)) {
    case -1:
        lua_pushboolean(L, 0);
        lua_pushstring(L, PQerrorMessage(conn));
        return 2;

    case 0:
        // no buffer space available, should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        lua_pushboolean(L, 1);
        return 1;
    }
}

static int put_copy_data_lua(lua_State *L)
{
    PGconn *conn       = libpq_check_conn(L);
    size_t nbytes      = 0;
    const char *buffer = lauxh_checklstring(L, 2, &nbytes);

    switch (PQputCopyData(conn, buffer, nbytes)) {
    case -1:
        lua_pushboolean(L, 0);
        lua_pushstring(L, PQerrorMessage(conn));
        return 2;

    case 0:
        // no buffer space available, should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        // queued
        lua_pushboolean(L, 1);
        return 1;
    }
}

static int notifies_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQconsumeInput(conn)) {
        PGnotify **notify = libpq_notify_new(L);

        *notify = PQnotifies(conn);
        if (*notify) {
            lua_createtable(L, 0, 3);
            lauxh_pushstr2tbl(L, "relname", (*notify)->relname);
            lauxh_pushstr2tbl(L, "extra", (*notify)->extra);
            lauxh_pushint2tbl(L, "be_pid", (*notify)->be_pid);
            return 1;
        }
        lua_pushnil(L);
        return 1;
    }
    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int send_flush_request_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQsendFlushRequest(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int pipeline_sync_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQpipelineSync(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int exit_pipeline_mode_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQexitPipelineMode(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int enter_pipeline_mode_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQenterPipelineMode(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int consume_input_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQconsumeInput(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int is_busy_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQconsumeInput(conn)) {
        lua_pushboolean(L, PQisBusy(conn));
        return 1;
    }
    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int get_result_lua(lua_State *L)
{
    PGconn *conn   = libpq_check_conn(L);
    PGresult **res = libpq_result_new(L, 1, 0);
    char *errmsg   = NULL;

    *res = PQgetResult(conn);
    if (*res) {
        return 1;
    }
    errmsg = PQerrorMessage(conn);
    // got error
    if (errmsg && *errmsg) {
        lua_pushnil(L);
        lua_pushstring(L, errmsg);
        return 2;
    }
    return 0;
}

static int set_single_row_mode_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushboolean(L, PQsetSingleRowMode(conn));
    return 1;
}

static int send_query_params_lua(lua_State *L)
{
    int nparams         = lua_gettop(L) - 2;
    PGconn *conn        = libpq_check_conn(L);
    const char *command = lauxh_checkstring(L, 2);
    const char **params = NULL;

    if (nparams) {
        params = lua_newuserdata(L, sizeof(char *) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            params[i] = libpq_param2string(L, j);
        }
    }

    if (PQsendQueryParams(conn, command, nparams, NULL, params, NULL, NULL,
                          0)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int send_query_lua(lua_State *L)
{
    PGconn *conn      = libpq_check_conn(L);
    const char *query = lauxh_checkstring(L, 2);

    if (PQsendQuery(conn, query)) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int exec_params_lua(lua_State *L)
{
    int nparams         = lua_gettop(L) - 2;
    PGconn *conn        = libpq_check_conn(L);
    const char *command = lauxh_checkstring(L, 2);
    const char **params = NULL;
    PGresult **res      = NULL;

    if (nparams) {
        params = lua_newuserdata(L, sizeof(char *) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            params[i] = libpq_param2string(L, j);
        }
    }

    res  = libpq_result_new(L, 1, 0);
    *res = PQexecParams(conn, command, nparams, NULL, params, NULL, NULL, 0);
    if (*res) {
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int exec_lua(lua_State *L)
{
    PGconn *conn        = libpq_check_conn(L);
    const char *command = lauxh_checkstring(L, 2);
    PGresult **res      = libpq_result_new(L, 1, 0);

    *res = PQexec(conn, command);
    if (*res) {
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int set_trace_flags_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    int flags    = lauxh_optflags(L, 2);

    PQsetTraceFlags(conn, flags);
    return 0;
}

static int untrace_lua(lua_State *L)
{
    conn_t *c = checkself(L);

    PQuntrace(c->conn);
    if (c->trace_ref == LUA_NOREF) {
        lua_pushnil(L);
    } else {
        lauxh_pushref(L, c->trace_ref);
        c->trace_ref = lauxh_unref(L, c->trace_ref);
    }
    return 1;
}

static int trace_lua(lua_State *L)
{
    conn_t *c        = checkself(L);
    FILE *debug_port = lauxh_checkfile(L, 2);

    // remove old file
    untrace_lua(L);
    // set new file
    c->trace_ref = lauxh_refat(L, 2);
    PQtrace(c->conn, debug_port);

    return 1;
}

static int call_notice_receiver_lua(lua_State *L)
{
    conn_t *c = checkself(L);

    luaL_checkudata(L, 2, LIBPQ_RESULT_MT);
    lua_settop(L, 2);
    if (c->notice_recv_ref == LUA_NOREF) {
        lua_pushboolean(L, 0);
        return 1;
    }

    // call closure
    lauxh_pushref(L, c->notice_recv_ref);
    lua_insert(L, 2);
    lua_call(L, 1, 0);
    lua_pushboolean(L, 1);
    return 1;
}

static int call_notice_processor_lua(lua_State *L)
{
    conn_t *c = checkself(L);

    luaL_checktype(L, 2, LUA_TSTRING);
    lua_settop(L, 2);
    if (c->notice_proc_ref == LUA_NOREF) {
        lua_pushboolean(L, 0);
        return 1;
    }

    // call closure
    lauxh_pushref(L, c->notice_proc_ref);
    lua_insert(L, 2);
    lua_call(L, 1, 0);
    lua_pushboolean(L, 1);
    return 1;
}

static int notice_closure(lua_State *L)
{
    int narg = lua_gettop(L);
    int farg = 0;

    // get number of arguments
    lua_pushvalue(L, lua_upvalueindex(1));
    farg = lua_tointeger(L, -1);
    lua_pop(L, 1);

    // push function
    luaL_checkstack(L, farg + narg, "failed to call the notice function");
    lua_pushvalue(L, lua_upvalueindex(2));
    if (narg) {
        lua_insert(L, 1);
    }
    // push upvalues to function arguments
    for (int i = 1; i <= farg; i++) {
        lua_pushvalue(L, lua_upvalueindex(2 + i));
        if (narg) {
            lua_insert(L, 1 + i);
        }
    }
    lua_call(L, farg + narg, 0);

    return 0;
}

static inline void push_notice_closure(lua_State *L)
{
    int argc = lua_gettop(L) - 2;

    // create closure function
    luaL_checktype(L, 2, LUA_TFUNCTION);
    // The first argument sets the number of arguments, the second sets the lua
    // function, and the rest are used as function arguments.
    lua_pushinteger(L, argc);
    lua_insert(L, 2);
    lua_pushcclosure(L, notice_closure, 2 + argc);
}

#define set_notice_closure(L, type, register_fn)                               \
 do {                                                                          \
  conn_t *c = checkself((L));                                                  \
  /* release old reference */                                                  \
  if (c->notice_##type##_ref != LUA_NOREF) {                                   \
   c->notice_##type##_ref = lauxh_unref((L), c->notice_##type##_ref);          \
  }                                                                            \
                                                                               \
  if (!lua_isnoneornil((L), 2)) {                                              \
   /* push closure function */                                                 \
   push_notice_closure((L));                                                   \
   c->notice_##type##_ref = lauxh_ref((L));                                    \
   if (c->default_##type == NULL) {                                            \
    /* set custom notice function */                                           \
    c->default_##type = register_fn(c->conn, notice_##type, c);                \
   }                                                                           \
  } else if (c->default_##type) {                                              \
   /* set default notice function */                                           \
   register_fn(c->conn, c->default_##type, NULL);                              \
   c->default_##type = NULL;                                                   \
  }                                                                            \
 } while (0)

static void notice_recv(void *arg, const PGresult *res)
{
    conn_t *c = (conn_t *)arg;

    // call closure
    lauxh_pushref(c->L, c->notice_recv_ref);
    *libpq_result_new(c->L, 1, 1) = (PGresult *)res;
    lua_call(c->L, 1, 0);
}

static int set_notice_receiver_lua(lua_State *L)
{
    set_notice_closure(L, recv, PQsetNoticeReceiver);
    return 0;
}

static void notice_proc(void *arg, const char *message)
{
    conn_t *c = (conn_t *)arg;

    // call closure
    lauxh_pushref(c->L, c->notice_proc_ref);
    lua_pushstring(c->L, message);
    lua_call(c->L, 1, 0);
}

static int set_notice_processor_lua(lua_State *L)
{
    set_notice_closure(L, proc, PQsetNoticeProcessor);
    return 0;
}

static int set_error_context_visibility_lua(lua_State *L)
{
    PGconn *conn   = libpq_check_conn(L);
    int visibility = lauxh_checkinteger(L, 2);
    lua_pushinteger(L, PQsetErrorContextVisibility(conn, visibility));
    return 1;
}

static int set_error_verbosity_lua(lua_State *L)
{
    PGconn *conn  = libpq_check_conn(L);
    int verbosity = lauxh_checkinteger(L, 2);
    lua_pushinteger(L, PQsetErrorVerbosity(conn, verbosity));
    return 1;
}

static int ssl_attribute_names_lua(lua_State *L)
{
    PGconn *conn             = libpq_check_conn(L);
    const char *const *names = PQsslAttributeNames(conn);
    int i                    = 1;

    lua_newtable(L);
    while (*names) {
        lauxh_pushstr2arr(L, i++, *names);
        names++;
    }

    return 1;
}

static int ssl_attribute_lua(lua_State *L)
{
    PGconn *conn               = libpq_check_conn(L);
    const char *attribute_name = lauxh_checkstring(L, 2);
    lua_pushstring(L, PQsslAttribute(conn, attribute_name));
    return 1;
}

static int ssl_in_use_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushboolean(L, PQsslInUse(conn));
    return 1;
}

static int set_client_encoding_lua(lua_State *L)
{
    PGconn *conn         = libpq_check_conn(L);
    const char *encoding = lauxh_checkstring(L, 2);

    if (PQsetClientEncoding(conn, encoding) == 0) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int client_encoding_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, pg_encoding_to_char(PQclientEncoding(conn)));
    return 1;
}

static int connection_used_password_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushboolean(L, PQconnectionUsedPassword(conn));
    return 1;
}

static int connection_needs_password_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushboolean(L, PQconnectionNeedsPassword(conn));
    return 1;
}

static int pipeline_status_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQpipelineStatus(conn));
    return 1;
}

static int backend_pid_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQbackendPID(conn));
    return 1;
}

static int socket_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQsocket(conn));
    return 1;
}

static int error_message_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    char *err    = PQerrorMessage(conn);

    if (err && *err) {
        lua_pushstring(L, PQerrorMessage(conn));
        return 1;
    }
    return 0;
}

static int server_version_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQserverVersion(conn));
    return 1;
}

static int protocol_version_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQprotocolVersion(conn));
    return 1;
}

static int parameter_status_lua(lua_State *L)
{
    PGconn *conn          = libpq_check_conn(L);
    const char *paramName = lauxh_optstring(L, 2, NULL);
    lua_pushstring(L, PQparameterStatus(conn, paramName));
    return 1;
}

static int transaction_status_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQtransactionStatus(conn));
    return 1;
}

static int status_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushinteger(L, PQstatus(conn));
    return 1;
}

static int options_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQoptions(conn));
    return 1;
}

static int port_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQport(conn));
    return 1;
}

static int hostaddr_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQhostaddr(conn));
    return 1;
}

static int host_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQhost(conn));
    return 1;
}

static int pass_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQpass(conn));
    return 1;
}

static int user_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQuser(conn));
    return 1;
}

static int db_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);
    lua_pushstring(L, PQdb(conn));
    return 1;
}

static int request_cancel_lua(lua_State *L)
{
    PGconn *conn = libpq_check_conn(L);

    if (PQrequestCancel(libpq_check_conn(L))) {
        lua_pushboolean(L, 1);
        return 1;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_pushstring(L, PQerrorMessage(conn));
    return 2;
}

static int get_cancel_lua(lua_State *L)
{
    PGcancel **cancel = libpq_cancel_new(L);

    *cancel = PQgetCancel(libpq_check_conn(L));
    if (*cancel) {
        return 1;
    }
    lua_pushnil(L);
    lua_errno_new(L, errno, "get_cancel");
    return 2;
}

static int connect_poll_lua(lua_State *L)
{
    lua_pushinteger(L, PQconnectPoll(libpq_check_conn(L)));
    return 1;
}

static inline void push_conninfo_options(lua_State *L,
                                         PQconninfoOption *options)
{
    PQconninfoOption *opt = options;

    lua_newtable(L);
    while (opt->keyword) {
        // The keyword of the option
        lua_pushstring(L, opt->keyword);
        lua_createtable(L, 0, 6);
        // Fallback environment variable name
        lauxh_pushstr2tbl(L, "envvar", opt->envvar);
        // Fallback compiled in default value
        lauxh_pushstr2tbl(L, "compiled", opt->compiled);
        // Option's current value, or NULL
        lauxh_pushstr2tbl(L, "val", opt->val);
        // Label for field in connect dialog
        lauxh_pushstr2tbl(L, "label", opt->label);
        // Indicates how to display this field in a
        // connect dialog. Values are: "" Display
        // entered value as is "*" Password field -
        // hide value "D"  Debug option - don't show
        // by default
        lauxh_pushstr2tbl(L, "dispchar", opt->dispchar);
        /* Field size in characters for dialog	*/
        lauxh_pushint2tbl(L, "dispsize", opt->dispsize);
        lua_rawset(L, -3);
        opt++;
    }
}

static int conninfo_lua(lua_State *L)
{
    PQconninfoOption *options = PQconninfo(libpq_check_conn(L));

    if (options) {
        push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "conninfo");
    return 2;
}

static inline int finish(lua_State *L)
{
    conn_t *c = luaL_checkudata(L, 1, LIBPQ_CONN_MT);

    if (c->conn) {
        PQfinish(c->conn);
        c->conn = NULL;
        lauxh_unref(L, c->notice_recv_ref);
        lauxh_unref(L, c->notice_proc_ref);
        lauxh_unref(L, c->trace_ref);
    }

    return 0;
}

static int finish_lua(lua_State *L)
{
    return finish(L);
}

static int gc_lua(lua_State *L)
{
    return finish(L);
}

static int tostring_lua(lua_State *L)
{
    return libpq_tostring(L, LIBPQ_CONN_MT);
}

static int connect_lua(lua_State *L)
{
    const char *conninfo = lauxh_optstring(L, 1, "");
    int nonblock         = lauxh_optboolean(L, 2, 0);
    conn_t *c            = lua_newuserdata(L, sizeof(conn_t));

    *c = (conn_t){
        .L               = L,
        .notice_recv_ref = LUA_NOREF,
        .notice_proc_ref = LUA_NOREF,
        .trace_ref       = LUA_NOREF,
    };

    if (nonblock) {
        c->conn = PQconnectStart(conninfo);
    } else {
        c->conn = PQconnectdb(conninfo);
    }

    if (c->conn) {
        lauxh_setmetatable(L, LIBPQ_CONN_MT);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "connect");
    return 2;
}

static int ping_lua(lua_State *L)
{
    const char *conninfo = lauxh_optstring(L, 1, "");
    lua_pushinteger(L, PQping(conninfo));
    return 1;
}

static int parse_conninfo_lua(lua_State *L)
{
    const char *conninfo      = lauxh_checkstring(L, 1);
    char *errmsg              = NULL;
    PQconninfoOption *options = PQconninfoParse(conninfo, &errmsg);

    if (options) {
        push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_pushstring(L, errmsg);
    return 2;
}

static int default_conninfo_lua(lua_State *L)
{
    PQconninfoOption *options = PQconndefaults();

    if (options) {
        push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "default_conninfo");
    return 2;
}

void libpq_conn_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"finish",                       finish_lua                      },
        {"conninfo",                     conninfo_lua                    },
        {"connect_poll",                 connect_poll_lua                },
        {"get_cancel",                   get_cancel_lua                  },
        {"request_cancel",               request_cancel_lua              },
        {"db",                           db_lua                          },
        {"user",                         user_lua                        },
        {"pass",                         pass_lua                        },
        {"host",                         host_lua                        },
        {"hostaddr",                     hostaddr_lua                    },
        {"port",                         port_lua                        },
        {"options",                      options_lua                     },
        {"status",                       status_lua                      },
        {"transaction_status",           transaction_status_lua          },
        {"parameter_status",             parameter_status_lua            },
        {"protocol_version",             protocol_version_lua            },
        {"server_version",               server_version_lua              },
        {"error_message",                error_message_lua               },
        {"socket",                       socket_lua                      },
        {"backend_pid",                  backend_pid_lua                 },
        {"pipeline_status",              pipeline_status_lua             },
        {"connection_needs_password",    connection_needs_password_lua   },
        {"connection_used_password",     connection_used_password_lua    },
        {"client_encoding",              client_encoding_lua             },
        {"set_client_encoding",          set_client_encoding_lua         },
        {"ssl_in_use",                   ssl_in_use_lua                  },
        {"ssl_attribute",                ssl_attribute_lua               },
        {"ssl_attribute_names",          ssl_attribute_names_lua         },
        {"set_error_verbosity",          set_error_verbosity_lua         },
        {"set_error_context_visibility", set_error_context_visibility_lua},
        {"set_notice_processor",         set_notice_processor_lua        },
        {"set_notice_receiver",          set_notice_receiver_lua         },
        {"call_notice_processor",        call_notice_processor_lua       },
        {"call_notice_receiver",         call_notice_receiver_lua        },
        {"trace",                        trace_lua                       },
        {"untrace",                      untrace_lua                     },
        {"set_trace_flags",              set_trace_flags_lua             },
        {"exec",                         exec_lua                        },
        {"exec_params",                  exec_params_lua                 },
        {"send_query",                   send_query_lua                  },
        {"send_query_params",            send_query_params_lua           },
        {"set_single_row_mode",          set_single_row_mode_lua         },
        {"get_result",                   get_result_lua                  },
        {"is_busy",                      is_busy_lua                     },
        {"consume_input",                consume_input_lua               },
        {"enter_pipeline_mode",          enter_pipeline_mode_lua         },
        {"exit_pipeline_mode",           exit_pipeline_mode_lua          },
        {"pipeline_sync",                pipeline_sync_lua               },
        {"send_flush_request",           send_flush_request_lua          },
        {"notifies",                     notifies_lua                    },
        {"put_copy_data",                put_copy_data_lua               },
        {"put_copy_end",                 put_copy_end_lua                },
        {"get_copy_data",                get_copy_data_lua               },
        {"set_nonblocking",              set_nonblocking_lua             },
        {"is_nonblocking",               is_nonblocking_lua              },
        {"flush",                        flush_lua                       },
        {"make_empty_result",            make_empty_result_lua           },
        {"escape_string_conn",           escape_string_conn_lua          },
        {"escape_literal",               escape_literal_lua              },
        {"escape_identifier",            escape_identifier_lua           },
        {"escape_bytea_conn",            escape_bytea_conn_lua           },
        {"encrypt_password_conn",        encrypt_password_conn_lua       },
        {NULL,                           NULL                            }
    };

    libpq_register_mt(L, LIBPQ_CONN_MT, mmethod, method);

    // create module table
    lauxh_pushfn2tbl(L, "default_conninfo", default_conninfo_lua);
    lauxh_pushfn2tbl(L, "parse_conninfo", parse_conninfo_lua);
    lauxh_pushfn2tbl(L, "ping", ping_lua);
    lauxh_pushfn2tbl(L, "connect", connect_lua);
}
