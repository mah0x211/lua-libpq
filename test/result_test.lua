local testcase = require('testcase')
local libpq = require('libpq')

function testcase.clear()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)

    -- test that clear result
    res:clear()
    -- cannot call a method after clear
    local err = assert.throws(res.status, res)
    assert.match(err, 'attempt to use a freed object')

    -- test that clear method can be called more than once
end

function testcase.get_attributes()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)

    res = assert(c:exec([[
        INSERT INTO copy_test (str, num)
        VALUES (
            'foo', 123
        ) RETURNING id
    ]]))

    assert.equal({
        res:status(),
    }, {
        libpq.RES_TUPLES_OK,
        'PGRES_TUPLES_OK',
    })
    assert.equal(res:error_message(), '')
    assert.match(res:verbose_error_message(), 'PGresult is not an error result')
    assert.equal(res:ntuples(), 1)
    assert.equal(res:nfields(), 1)
    assert.is_false(res:binary_tuples())
    assert.equal(res:fname(1), 'id')
    assert.equal(res:fnumber('id'), 1)
    assert.is_int(res:ftable(1))
    assert.equal(res:ftablecol(1), 1)
    assert.equal(res:fformat(1), 0)
    assert.is_int(res:ftype(1))
    assert.greater(res:fsize(1), 0)
    assert.equal(res:fmod(1), -1)
    assert.equal(res:cmd_status(), 'INSERT 0 1')
    assert.is_int(res:oid_value(1))
    assert.equal(res:cmd_tuples(), 1)
    assert.equal(res:nparams(), 0)
end

function testcase.stat()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))

    -- test that stat table must contains: status, status_text, cmd_status
    local stat = assert(res:stat())
    assert.is_table(stat)
    assert.contains(stat, {
        status = libpq.RES_COMMAND_OK,
        status_text = 'PGRES_COMMAND_OK',
        cmd_status = 'CREATE TABLE',
    })

    -- test that stat table contains the RES_EMPTY_QUERY status
    res = assert(c:exec(''))
    stat = assert(res:stat())
    assert.equal(stat, {
        status = libpq.RES_EMPTY_QUERY,
        status_text = 'PGRES_EMPTY_QUERY',
        cmd_status = '',
    })

    -- test that stat table contains the RES_TUPLES_OK status
    res = assert(c:exec([[
        INSERT INTO test_tbl (str, num)
        VALUES (
            'foo', 123
        ), (
            'foo', 123
        ) RETURNING id
    ]]))
    stat = assert(res:stat())
    assert.contains(stat, {
        status = libpq.RES_TUPLES_OK,
        status_text = 'PGRES_TUPLES_OK',
        cmd_status = 'INSERT 0 2',
        cmd_tuples = 2,
        nfields = 1,
        ntuples = 2,
        oid_value = 0,
        fields = {
            {
                format = 0,
                name = 'id',
            },
        },
    })
end

function testcase.get_value()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)
    res = assert(c:exec([[
        INSERT INTO test_tbl (str, num)
        VALUES (
            'foo', 101
        ), (
            'bar', 102
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)

    -- test that get value of each row
    res = assert(c:exec([[
        SELECT * FROM test_tbl
    ]]))
    local stat = assert(res:stat())
    assert.equal(stat.status, libpq.RES_TUPLES_OK)

    local rows = {}
    for row = 1, stat.ntuples do
        local line = {}
        for col = 1, stat.nfields do
            if not res:get_is_null(row, col) then
                local len = res:get_length(row, col)
                local val = res:get_value(row, col)
                assert.equal(len, #val)
                line[col] = val
            end
        end
        rows[#rows + 1] = line
    end
    assert.equal(rows, {
        {
            '1',
            'foo',
            '101',
        },
        {
            '2',
            'bar',
            '102',
        },
    })
end

