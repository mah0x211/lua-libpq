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

static int gc_lua(lua_State *L)
{
    PGnotify **notify = luaL_checkudata(L, 1, LIBPQ_NOTIFY_MT);

    if (*notify) {
        PQfreemem(*notify);
        *notify = NULL;
    }

    return 0;
}

PGnotify **libpq_notify_new(lua_State *L)
{
    PGnotify **notify = lua_newuserdata(L, sizeof(PGnotify *));
    *notify           = NULL;
    lauxh_setmetatable(L, LIBPQ_NOTIFY_MT);
    return notify;
}

void libpq_notify_init(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc", gc_lua},
        {NULL,   NULL  }
    };
    struct luaL_Reg method[] = {
        {NULL, NULL}
    };

    libpq_register_mt(L, LIBPQ_NOTIFY_MT, mmethod, method);
}
