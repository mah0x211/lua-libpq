local testcase = require('testcase')
local libpq = require('libpq')

function testcase.get_result_stat()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))

    -- test that stat table must contains: status, status_text, cmd_status
    local stat = assert(libpq.util.get_result_stat(res))
    assert.is_table(stat)
    assert.contains(stat, {
        status = libpq.RES_COMMAND_OK,
        status_text = 'PGRES_COMMAND_OK',
        cmd_status = 'CREATE TABLE',
    })

    -- test that stat table contains the RES_EMPTY_QUERY status
    res = assert(c:exec(''))
    stat = assert(libpq.util.get_result_stat(res))
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
    stat = assert(libpq.util.get_result_stat(res))
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

function testcase.get_result_rows()
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

    -- test that get result rows
    res = assert(c:exec([[
        SELECT * FROM test_tbl
    ]]))
    assert.equal(res:status(), libpq.RES_TUPLES_OK)
    local rows = libpq.util.get_result_rows(res)
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

function testcase.iterate_result_rows()
    local c = assert(libpq.connect())
    local res = assert(c:exec([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)

    -- test that no rows
    res = assert(c:exec([[
        INSERT INTO test_tbl (str, num)
        VALUES (
            'foo', 101
        ), (
            'bar', 102
        )
    ]]))
    assert.equal(res:status(), libpq.RES_COMMAND_OK)
    local rows = {}
    for n, row in libpq.util.iterate_result_rows(res) do
        rows[n] = row
    end
    assert.empty(rows)

    -- test that iterates the result rows
    res = assert(c:exec([[
        SELECT * FROM test_tbl
    ]]))
    assert.equal(res:status(), libpq.RES_TUPLES_OK)
    rows = {}
    for n, row in libpq.util.iterate_result_rows(res) do
        rows[n] = row
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

    -- test that iterates the result rows in single row mode
    assert(c:send_query([[
        SELECT * FROM test_tbl
    ]]))
    assert(c:set_single_row_mode())
    rows = {}
    local err
    res, err = c:get_result()
    while res do
        for _, row in libpq.util.iterate_result_rows(res) do
            rows[#rows + 1] = row
        end

        res, err = c:get_result()
    end
    if err then
        error(err)
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

function testcase.iterate_result_rows_in_single_row_mode()
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

    -- test that iterates the result rows in single row mode
    assert(c:send_query([[
        SELECT * FROM test_tbl
    ]]))
    assert(c:set_single_row_mode())
    local rows = {}
    local err
    res, err = c:get_result()
    while res do
        for _, row in libpq.util.iterate_result_rows(res) do
            rows[#rows + 1] = row
        end
        res, err = c:get_result()
    end
    if err then
        error(err)
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
