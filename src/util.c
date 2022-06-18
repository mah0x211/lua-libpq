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

static int iterate_result_rows(lua_State *L)
{
    const PGresult *res = libpq_check_result(L);
    int n               = lauxh_optpinteger(L, 2, 0);
    int nrow            = PQntuples(res);

    if (n < nrow) {
        int ncol = PQnfields(res);

        lua_settop(L, 1);
        lua_pushinteger(L, n + 1);
        lua_createtable(L, ncol, 0);
        for (int i = 0; i < ncol; i++) {
            if (!PQgetisnull(res, n, i)) {
                lauxh_pushstr2arr(L, i + 1, PQgetvalue(res, n, i));
            }
        }
        return 2;
    }
    // done
    return 0;
}

static int iterate_result_rows_lua(lua_State *L)
{
    int n = 0;

    libpq_check_result(L);
    n = lauxh_optpinteger(L, 2, 0);

    lua_settop(L, 1);
    lua_pushcclosure(L, iterate_result_rows, 0);
    lua_insert(L, 1);
    if (n < 1) {
        lua_pushnil(L);
    } else {
        lua_pushinteger(L, n);
    }
    return 3;
}

static int get_result_rows_lua(lua_State *L)
{
    const PGresult *res = libpq_check_result(L);
    int nrow            = PQntuples(res);
    int ncol            = PQnfields(res);

    lua_settop(L, 1);
    lua_createtable(L, nrow, 0);
    for (int row = 0; row < nrow; row++) {
        lua_createtable(L, ncol, 0);
        for (int col = 0; col < ncol; col++) {
            if (!PQgetisnull(res, row, col)) {
                lauxh_pushstr2arr(L, col + 1, PQgetvalue(res, row, col));
            }
        }
        lua_rawseti(L, -2, row + 1);
    }

    return 1;
}

static int get_result_stat_lua(lua_State *L)
{
    const PGresult *res   = libpq_check_result(L);
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
        uintmax_t cmd_tuples = libpq_str2uint(PQcmdTuples((PGresult *)res));

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

void libpq_util_init(lua_State *L)
{
    lua_createtable(L, 0, 1);
    lauxh_pushfn2tbl(L, "get_result_stat", get_result_stat_lua);
    lauxh_pushfn2tbl(L, "get_result_rows", get_result_rows_lua);
    lauxh_pushfn2tbl(L, "iterate_result_rows", iterate_result_rows_lua);
    lua_setfield(L, -2, "util");
}
