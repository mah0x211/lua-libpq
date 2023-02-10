package = "libpq"
version = "dev-1"
source = {
    url = "git+https://github.com/mah0x211/lua-libpq.git",
}
description = {
    summary = "libpq bindings for lua",
    homepage = "https://github.com/mah0x211/lua-libpq",
    license = "MIT/X11",
    maintainer = "Masatoshi Fukunaga",
}
dependencies = {
    "lua >= 5.1",
    "errno >= 0.3.0",
    "lauxhlib >= 0.5.0",
}
build = {
    type = "make",
    build_variables = {
        PACKAGE = "libpq",
        SRCDIR = "src",
        CFLAGS = "$(CFLAGS)",
        WARNINGS = "-Wall -Wno-trigraphs -Wmissing-field-initializers -Wreturn-type -Wmissing-braces -Wparentheses -Wno-switch -Wunused-function -Wunused-label -Wunused-parameter -Wunused-variable -Wunused-value -Wuninitialized -Wunknown-pragmas -Wshadow -Wsign-compare",
        CPPFLAGS = "-I$(LUA_INCDIR)",
        LDFLAGS = "$(LIBFLAG)",
        LIB_EXTENSION = "$(LIB_EXTENSION)",
        LIBPQ_COVERAGE = "$(LIBPQ_COVERAGE)",
    },
    install_variables = {
        PACKAGE = "libpq",
        SRCDIR = "src",
        INST_LIBDIR = "$(LIBDIR)",
        LIB_EXTENSION = "$(LIB_EXTENSION)",
    },
}
