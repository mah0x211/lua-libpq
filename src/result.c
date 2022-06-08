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
#include <inttypes.h>

typedef struct {
    PGresult *res;
    int noclear;
} result_t;

static inline result_t *checkself(lua_State *L)
{
    result_t *r = luaL_checkudata(L, 1, LIBPQ_RESULT_MT);
    if (!r->res) {
        luaL_error(L, "attempt to use a freed object");
    }
    return r;
}

static inline PGresult *getresult(lua_State *L)
{
    result_t *r = checkself(L);
    return r->res;
}

static int param_type_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int param_num       = lauxh_checkinteger(L, 2);

    lua_pushinteger(L, PQparamtype(res, param_num));
    return 1;
}

static int nparams_lua(lua_State *L)
{
    const PGresult *res = getresult(L);

    lua_pushinteger(L, PQnparams(res));
    return 1;
}

static int get_is_null_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;

    lua_pushboolean(L, PQgetisnull(res, row, col));
    return 1;
}

static int get_length_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;

    lua_pushinteger(L, PQgetlength(res, row, col));
    return 1;
}

static int get_value_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;

    lua_pushstring(L, PQgetvalue(res, row, col));
    return 1;
}

static inline uintmax_t get_cmd_tuples(PGresult *res)
{
    char *cmd_tuples = PQcmdTuples(res);

    errno = 0;
    if (*cmd_tuples) {
        return strtoumax(cmd_tuples, NULL, 10);
    }

    return UINTMAX_MAX;
}

static int cmd_tuples_lua(lua_State *L)
{
    PGresult *res        = getresult(L);
    uintmax_t cmd_tuples = get_cmd_tuples(res);

    if (cmd_tuples != UINTMAX_MAX) {
        lua_pushinteger(L, cmd_tuples);
    } else {
        lua_pushnil(L);
    }
    return 1;
}

static int oid_value_lua(lua_State *L)
{
    const PGresult *res = getresult(L);

    lua_pushinteger(L, PQoidValue(res));
    return 1;
}

static int cmd_status_lua(lua_State *L)
{
    PGresult *res = getresult(L);

    lua_pushstring(L, PQcmdStatus(res));
    return 1;
}

static int fmod_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    lua_pushinteger(L, PQfmod(res, col));
    return 1;
}

static int fsize_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    lua_pushinteger(L, PQfsize(res, col));
    return 1;
}

static int ftype_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    lua_pushinteger(L, PQftype(res, col));
    return 1;
}

static int fformat_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    lua_pushinteger(L, PQfformat(res, col));
    return 1;
}

static int ftablecol_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    lua_pushinteger(L, PQftablecol(res, col));
    return 1;
}

static int ftable_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the table OID  column number of the specified column name
    lua_pushinteger(L, PQftable(res, col));
    return 1;
}

static int fnumber_lua(lua_State *L)
{
    const PGresult *res  = getresult(L);
    const char *col_name = lauxh_checkstring(L, 2);
    // get the column number of the specified column name
    int col              = PQfnumber(res, col_name);

    lua_pushinteger(L, (col != 1) ? col + 1 : -1);
    return 1;
}

static int fname_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the column name of the specified column number
    lua_pushstring(L, PQfname(res, col));
    return 1;
}

static int binary_tuples_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    // whether all column data are binary or not
    lua_pushboolean(L, PQbinaryTuples(res));
    return 1;
}

static int nfields_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    // get number of columns
    lua_pushinteger(L, PQnfields(res));
    return 1;
}

static int ntuples_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    // get number of rows
    lua_pushinteger(L, PQntuples(res));
    return 1;
}

static int error_field_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int fieldcode       = lauxh_checkinteger(L, 2);

    lua_pushstring(L, PQresultErrorField(res, fieldcode));
    return 1;
}

static int verbose_error_message_lua(lua_State *L)
{
    const PGresult *res = getresult(L);
    int verbosity       = lauxh_optinteger(L, 2, PQERRORS_DEFAULT);
    int show_context    = lauxh_optinteger(L, 3, PQSHOW_CONTEXT_ERRORS);
    char *msg = PQresultVerboseErrorMessage(res, verbosity, show_context);

    if (msg) {
        lua_pushstring(L, msg);
        PQfreemem(msg);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "verbose_error_message");
    return 2;
}

static int error_message_lua(lua_State *L)
{
    const PGresult *res = getresult(L);

    lua_pushstring(L, PQresultErrorMessage(res));
    return 1;
}

static int status_lua(lua_State *L)
{
    const PGresult *res   = getresult(L);
    ExecStatusType status = PQresultStatus(res);

    lua_pushinteger(L, status);
    lua_pushstring(L, PQresStatus(status));
    return 2;
}

// NOTE: helper functions
static int stat_lua(lua_State *L)
{
    const PGresult *res   = getresult(L);
    ExecStatusType status = PQresultStatus(res);

    lua_createtable(L, 0, 9);
    lauxh_pushint2tbl(L, "status", status);
    lauxh_pushstr2tbl(L, "status_text", PQresStatus(status));
    lauxh_pushstr2tbl(L, "cmd_status", PQcmdStatus((PGresult *)res));

    switch (status) {
    case PGRES_SINGLE_TUPLE: // single tuple from larger resultset
    case PGRES_TUPLES_OK: {  // a query command that returns tuples was executed
                             // properly by the backend, PGresult contains the
                             // result tuples
        int ntuples = PQntuples(res);
        lauxh_pushint2tbl(L, "ntuples", ntuples);
        if (ntuples) {
            int nfields = PQnfields(res);
            lauxh_pushint2tbl(L, "nfields", nfields);
            lauxh_pushint2tbl(L, "binary_tuples", PQbinaryTuples(res));
            lua_createtable(L, nfields, 0);
            for (int col = 0; col < nfields; col++) {
                lua_createtable(L, 0, 7);
                lauxh_pushstr2tbl(L, "name", PQfname(res, col));
                lauxh_pushint2tbl(L, "table", PQftable(res, col));
                lauxh_pushint2tbl(L, "tablecol", PQftablecol(res, col));
                lauxh_pushint2tbl(L, "format", PQfformat(res, col));
                lauxh_pushint2tbl(L, "type", PQftype(res, col));
                lauxh_pushint2tbl(L, "size", PQfsize(res, col));
                lauxh_pushint2tbl(L, "mod", PQfmod(res, col));
                lua_rawseti(L, -2, col + 1);
            }
            lua_setfield(L, -2, "fields");
        }
    } // fallthrough

    case PGRES_COMMAND_OK: { // a query command that doesn't return anything was
                             // executed properly by the backend
        int nparams          = PQnparams(res);
        uintmax_t cmd_tuples = get_cmd_tuples((PGresult *)res);

        if (cmd_tuples != UINTMAX_MAX) {
            lauxh_pushint2tbl(L, "cmd_tuples", cmd_tuples);
        }
        lauxh_pushint2tbl(L, "oid_value", PQoidValue(res));
        if (nparams) {
            lauxh_pushint2tbl(L, "nparams", nparams);
            lua_createtable(L, nparams, 0);
            for (int i = 0; i < nparams; i++) {
                lauxh_pushint2arr(L, i + 1, PQparamtype(res, i));
            }
            lua_setfield(L, -2, "params");
        }
    } // fallthrough

    case PGRES_EMPTY_QUERY:   // empty query string was executed
    case PGRES_PIPELINE_SYNC: // pipeline synchronization point
    case PGRES_COPY_OUT:      // Copy Out data transfer in progress
    case PGRES_COPY_IN:       // Copy In data transfer in progress
    case PGRES_COPY_BOTH:     // Copy In/Out data transfer in progress
        break;

    case PGRES_PIPELINE_ABORTED: // Command didn't run because of an abort
                                 // earlier in a pipeline
    case PGRES_BAD_RESPONSE:     // an unexpected response was recv'd from the
                                 // backend
    case PGRES_NONFATAL_ERROR:   // notice or warning message
    case PGRES_FATAL_ERROR:      // query failed
    default:
        lauxh_pushstr2tbl(L, "error", PQresultErrorMessage(res));
    }

    return 1;
}

static inline int clear(lua_State *L)
{
    result_t *r = luaL_checkudata(L, 1, LIBPQ_RESULT_MT);

    if (!r->noclear && r->res) {
        PQclear(r->res);
        r->res = NULL;
    }

    return 0;
}

static int clear_lua(lua_State *L)
{
    return clear(L);
}

static int gc_lua(lua_State *L)
{
    return clear(L);
}

static int tostring_lua(lua_State *L)
{
    return libpq_tostring(L, LIBPQ_RESULT_MT);
}

PGresult **libpq_result_new(lua_State *L, int noclear)
{
    result_t *r = lua_newuserdata(L, sizeof(result_t));
    r->res      = NULL;
    r->noclear  = noclear;
    lauxh_setmetatable(L, LIBPQ_RESULT_MT);
    return &r->res;
}

void libpq_result_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"clear",                 clear_lua                },
        {"stat",                  stat_lua                 },
        {"status",                status_lua               },
        {"error_message",         error_message_lua        },
        {"verbose_error_message", verbose_error_message_lua},
        {"error_field",           error_field_lua          },
        {"ntuples",               ntuples_lua              },
        {"nfields",               nfields_lua              },
        {"binary_tuples",         binary_tuples_lua        },
        {"fname",                 fname_lua                },
        {"fnumber",               fnumber_lua              },
        {"ftable",                ftable_lua               },
        {"ftablecol",             ftablecol_lua            },
        {"fformat",               fformat_lua              },
        {"ftype",                 ftype_lua                },
        {"fsize",                 fsize_lua                },
        {"fmod",                  fmod_lua                 },
        {"cmd_status",            cmd_status_lua           },
        {"oid_value",             oid_value_lua            },
        {"cmd_tuples",            cmd_tuples_lua           },
        {"get_value",             get_value_lua            },
        {"get_length",            get_length_lua           },
        {"get_is_null",           get_is_null_lua          },
        {"nparams",               nparams_lua              },
        {"param_type",            param_type_lua           },
        {NULL,                    NULL                     }
    };

    libpq_register_mt(L, LIBPQ_RESULT_MT, mmethod, method);
}
