std = "max"
include_files = {
    "test/*_test.lua",
}
exclude_files = {
    "_*.lua",
}
ignore = {
    'assert',
    -- unused argument
    '212',
    -- -- unused loop variable
    -- '213',
    -- -- value assigned to a local variable is unused
    -- '311',
    -- -- redefining a local variable
    -- '411',
    -- -- shadowing a local variable
    -- '421',
    -- line is too long
    '631',
}
