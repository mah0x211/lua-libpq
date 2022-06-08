local testcase = require('testcase')
local setenv = require('setenv')
local libpq = require('libpq')

function testcase.is_threadsafe()
    -- test that return a boolean value
    assert.is_boolean(libpq.is_threadsafe())
end

function testcase.lib_version()
    -- test that get library version
    local ver = libpq.lib_version()
    assert.is_uint(ver)
    assert.equal(math.floor(ver / 10000), 14)
end

function testcase.mblen()
    -- test that determine length of multibyte encoded char at *s
    assert.equal(libpq.mblen('abc', libpq.char_to_encoding('UTF-8')), 1)
    assert.equal(libpq.mblen('あabc', libpq.char_to_encoding('UTF-8')), 3)
end

function testcase.mblen_bounded()
    -- test that same of mblen, but not more than the distance to the end of string s
    assert.equal(libpq.mblen_bounded('abc', libpq.char_to_encoding('UTF-8')), 1)
    assert.equal(libpq.mblen_bounded('あabc', libpq.char_to_encoding('UTF-8')),
                 3)
end

function testcase.dsplen()
    -- test that determine display length of multibyte encoded char at *s
    assert.is_int(libpq.dsplen('abc', libpq.char_to_encoding('UTF-8')))
    assert.is_int(libpq.dsplen('あabc', libpq.char_to_encoding('UTF-8')))
end

function testcase.env2encoding()
    -- test that get an encoding id from PGCLIENTENCODING envvar
    assert.equal(libpq.env2encoding(), 0)
    setenv('PGCLIENTENCODING', 'UTF-8')
    assert.not_equal(libpq.char_to_encoding('UTF-8'), 0)
    assert.equal(libpq.env2encoding(), libpq.char_to_encoding('UTF-8'))
end

function testcase.encrypt_password()
    -- test that encrypt a string of password/user combinations
    assert.match(libpq.encrypt_password('foo', 'bar'), '^md5[a-f0-9]+', false)
end

function testcase.char_to_encoding()
    -- test that get a encoding type as string from
    assert.equal(libpq.char_to_encoding('SQL_ASCII'), 0)
end

function testcase.encoding_to_char()
    -- test that get a encoding type as string from
    assert.equal(libpq.encoding_to_char(0), 'SQL_ASCII')
end

function testcase.valid_server_encoding_id()
    -- test that get a encoding type as string from
    assert.is_true(libpq.valid_server_encoding_id(0))
    assert.is_false(libpq.valid_server_encoding_id(-123))
end

