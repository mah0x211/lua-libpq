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

#ifndef lua_libpq_h
#define lua_libpq_h

#include <inttypes.h>
#include <string.h>
// libpq
#include <libpq-fe.h>
#include <postgres_ext.h>
// lua
#include <lauxhlib.h>
#include <lua_errno.h>

#define LIBPQ_CONN_MT "libpq.conn"

void libpq_conn_init(lua_State *L);
PGconn *libpq_check_conn(lua_State *L);

#define LIBPQ_CANCEL_MT "libpq.cancel"
void libpq_cancel_init(lua_State *L);
PGcancel **libpq_cancel_new(lua_State *L);

#define LIBPQ_RESULT_MT "libpq.result"
void libpq_result_init(lua_State *L);
PGresult **libpq_result_new(lua_State *L, int conn_idx, int noclear);
PGresult *libpq_check_result(lua_State *L);

#define LIBPQ_NOTIFY_MT "libpq.notify"
void libpq_notify_init(lua_State *L);
PGnotify **libpq_notify_new(lua_State *L);

void libpq_util_init(lua_State *L);

static inline void libpq_register_mt(lua_State *L, const char *tname,
                                     struct luaL_Reg mmethod[],
                                     struct luaL_Reg method[])
{
    // create metatable
    luaL_newmetatable(L, tname);
    // metamethods
    for (struct luaL_Reg *ptr = mmethod; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    for (struct luaL_Reg *ptr = method; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    lua_rawset(L, -3);
    lua_pop(L, 1);
}

static inline int libpq_tostring(lua_State *L, const char *tname)
{
    void *p = luaL_checkudata(L, 1, tname);
    lua_pushfstring(L, "%s: %p", tname, p);
    return 1;
}

static inline const char *libpq_param2string(lua_State *L, int idx)
{
    int type = lua_type(L, idx);

    if (idx < 0) {
        idx = lua_gettop(L) + idx + 1;
    }

    if (type != LUA_TSTRING) {
        switch (type) {
        case LUA_TNONE:
        case LUA_TNIL:
            return NULL;

        case LUA_TBOOLEAN:
            if (lua_toboolean(L, idx)) {
                lua_pushliteral(L, "TRUE");
            } else {
                lua_pushliteral(L, "FALSE");
            }
            break;

        case LUA_TNUMBER:
            lua_pushstring(L, lua_tostring(L, idx));
            break;

        // case LUA_TTHREAD:
        // case LUA_TLIGHTUSERDATA:
        // case LUA_TTABLE:
        // case LUA_TFUNCTION:
        // case LUA_TUSERDATA:
        // case LUA_TTHREAD:
        default:
            lauxh_argerror(L, idx, "<%s> param is not supported",
                           luaL_typename(L, idx));
        }
        lua_replace(L, idx);
    }
    return lua_tostring(L, idx);
}

static inline uintmax_t libpq_str2uint(char *str)
{
    errno = 0;
    if (*str) {
        return strtoumax(str, NULL, 10);
    }
    return UINTMAX_MAX;
}

#endif
