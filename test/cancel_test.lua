local testcase = require('testcase')
local libpq = require('libpq')

function testcase.cancel()
    local c = assert(libpq.connect())
    local cancel = assert(c:get_cancel())

    -- test that return true even when the query is not in process
    assert.is_true(cancel:cancel())

    -- test that can be called more than once
    assert.is_true(cancel:cancel())
end

function testcase.free()
    local c = assert(libpq.connect())
    local cancel = assert(c:get_cancel())

    -- test that free the allocated memory
    cancel:free()

    -- test that can be called more than once
    cancel:free()

    -- test that cannot use freed cancel object
    local err = assert.throws(cancel.cancel, cancel)
    assert.match(err, 'attempt to use a freed object')
end

