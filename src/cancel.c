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

static inline PGcancel *checkself(lua_State *L)
{
    PGcancel **cancel = luaL_checkudata(L, 1, LIBPQ_CANCEL_MT);
    if (!*cancel) {
        luaL_error(L, "attempt to use a freed object");
    }
    return *cancel;
}

static int cancel_lua(lua_State *L)
{
    PGcancel *cancel = checkself(L);
    char errbuf[256] = {0};

    if (PQcancel(cancel, errbuf, sizeof(errbuf))) {
        lua_pushboolean(L, 1);
        return 1;
    }
    lua_pushboolean(L, 0);
    lua_pushstring(L, errbuf);
    return 2;
}

static inline int free_cancel(lua_State *L)
{
    PGcancel **cancel = luaL_checkudata(L, 1, LIBPQ_CANCEL_MT);

    if (*cancel) {
        PQfreeCancel(*cancel);
        *cancel = NULL;
    }

    return 0;
}

static int free_lua(lua_State *L)
{
    return free_cancel(L);
}

static int gc_lua(lua_State *L)
{
    return free_cancel(L);
}

static int tostring_lua(lua_State *L)
{
    return libpq_tostring(L, LIBPQ_CANCEL_MT);
}

PGcancel **libpq_cancel_new(lua_State *L)
{
    PGcancel **cancel = lua_newuserdata(L, sizeof(PGcancel *));
    *cancel           = NULL;
    lauxh_setmetatable(L, LIBPQ_CANCEL_MT);
    return cancel;
}

void libpq_cancel_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"free",   free_lua  },
        {"cancel", cancel_lua},
        {NULL,     NULL      }
    };

    libpq_register_mt(L, LIBPQ_CANCEL_MT, mmethod, method);
}
