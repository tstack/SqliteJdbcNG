
PRAGMA foreign_keys = ON;

CREATE TABLE metadata_function (
    FUNCTION_KIND VARCHAR(32) CHECK (FUNCTION_KIND IN ('numeric', 'string', 'system')),
    FUNCTION_NAME VARCHAR(64) UNIQUE,
    REMARKS TEXT
);

INSERT INTO metadata_function VALUES (
    'numeric', 'abs', 'Return the absolute value of the argument.'
);

INSERT INTO metadata_function VALUES (
    'system', 'changes', 'Return the number of database rows changed by the last DML statement.'
);

INSERT INTO metadata_function VALUES (
    'string', 'char', 'Return a string having the given unicode code points.'
);

INSERT INTO metadata_function VALUES (
    'system', 'coalesce', 'Returns a copy of the first non-NULL argument.'
);

INSERT INTO metadata_function VALUES (
    'string', 'glob', 'Test a string against a glob pattern.'
);

INSERT INTO metadata_function VALUES (
    'system', 'ifnull', 'Return a copy of the first non-NULL argument.'
);

INSERT INTO metadata_function VALUES (
    'string', 'instr', 'Find the first occurrence of one string in another.'
);

INSERT INTO metadata_function VALUES (
    'string', 'hex', 'Return the hexadecimal rendering of its argument.'
);

INSERT INTO metadata_function VALUES (
    'system', 'last_insert_rowid', 'Returns the ROWID of the last row inserted.'
);

INSERT INTO metadata_function VALUES (
    'string', 'length', 'Return the length of its argument.'
);

INSERT INTO metadata_function VALUES (
    'string', 'like', 'Test a string against a like pattern.'
);

INSERT INTO metadata_function VALUES (
    'system', 'load_extension', 'Load an extension module.'
);

INSERT INTO metadata_function VALUES (
    'string', 'lower', 'Return the lowercase version of its argument.'
);

INSERT INTO metadata_function VALUES (
    'string', 'ltrim', 'Trim the left side of a string.'
);

INSERT INTO metadata_function VALUES (
    'numeric', 'max', 'Return the maximum value of its arguments.'
);

INSERT INTO metadata_function VALUES (
    'numeric', 'min', 'Return the minimum value of its arguments.'
);

INSERT INTO metadata_function VALUES (
    'system', 'nullif', 'Conditionally return arguments if one is NULL.'
);

INSERT INTO metadata_function VALUES (
    'string', 'printf', 'The printf() C-language function.'
);

INSERT INTO metadata_function VALUES (
    'string', 'quote', 'Return the given string as a quoted SQL string literal.'
);

INSERT INTO metadata_function VALUES (
    'numeric', 'random', 'Return a random number.'
);

INSERT INTO metadata_function VALUES (
    'system', 'randomblob', 'Return an N-byte blob of pseudo-random bytes.'
);

INSERT INTO metadata_function VALUES (
    'string', 'replace', 'Perform substitutions on a string.'
);

INSERT INTO metadata_function VALUES (
    'numeric', 'round', 'Round a floating-point value.'
);

INSERT INTO metadata_function VALUES (
    'string', 'rtrim', 'Trim characters from the right hand side of a string.'
);

INSERT INTO metadata_function VALUES (
    'string', 'soundex', 'Return the soundex encoding of a string.'
);

INSERT INTO metadata_function VALUES (
    'system', 'sqlite_compileopiton_get', 'Get the compile-time options for the SQLite library.'
);

INSERT INTO metadata_function VALUES (
    'system', 'sqlite_compileoption_used', 'Test if a compile-time option was used.'
);

INSERT INTO metadata_function VALUES (
    'system', 'sqlite_source_id', 'Return the source SHA1 hash.'
);

INSERT INTO metadata_function VALUES (
    'system', 'sqlite_version', 'Return the version of the SQLite library.'
);

INSERT INTO metadata_function VALUES (
    'string', 'substr', 'Return a portion of a string.'
);

INSERT INTO metadata_function VALUES (
    'system', 'total_changes', 'Return the total number of changes caused by DML statements.'
);

INSERT INTO metadata_function VALUES (
    'string', 'trim', 'Trim characters from both sides of a string.'
);

INSERT INTO metadata_function VALUES (
    'system', 'typeof', 'Return the type of its argument.'
);

INSERT INTO metadata_function VALUES (
    'string', 'unicode', 'Return the numeric code point of the first character of string.'
);

INSERT INTO metadata_function VALUES (
    'string', 'upper', 'Return the uppercase version of a string.'
);

INSERT INTO metadata_function VALUES (
    'system', 'zeroblob', 'Return a blob consisting of N bytes of 0x00.'
);


CREATE TABLE metadata_function_arguments (
    FUNCTION_NAME VARCHAR(64),
    COLUMN_NAME VARCHAR(64),
    COLUMN_TYPE SMALLINT,
    TYPE_NAME VARCHAR(64),
    REMARKS TEXT,
    ORDINAL_POSITION INT CHECK (ORDINAL_POSITION >= 0),
    IS_NULLABLE VARCHAR(64) CHECK (IS_NULLABLE IN ('YES', 'NO', '')),

    UNIQUE (FUNCTION_NAME, COLUMN_NAME),
    UNIQUE (FUNCTION_NAME, ORDINAL_POSITION),

    FOREIGN KEY(TYPE_NAME) REFERENCES metadata_types(TYPE_NAME)
);

INSERT INTO metadata_function_arguments VALUES (
    'abs',
    'X', 1, 'NUMERIC',
    'The absolute value of this argument will be returned',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'abs',
    'return', 5, 'NUMERIC',
    'The absolute value of the X argument.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'changes',
    'return', 5, 'NUMERIC',
    'The number of database rows that were changed, inserted, or deleted.',
    1, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'char',
    'X', 1, 'INTEGER',
    'A unicode codepoint value.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'char',
    'return', 5, 'VARCHAR',
    'A string composed of the unicode point values as given by the arguments.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'coalesce',
    'X', 1, 'VARCHAR',
    'An argument to test for NULL-ness.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'coalesce',
    'Y', 1, 'VARCHAR',
    'An argument to test for NULL-ness.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'coalesce',
    'return', 5, 'VARCHAR',
    'The first non-NULL argument.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'glob',
    'PATTERN', 1, 'VARCHAR',
    'The glob pattern to test the string against.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'glob',
    'X', 1, 'VARCHAR',
    'The string to test against the pattern.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'glob',
    'return', 5, 'BOOLEAN',
    'One if the pattern matched and zero otherwise.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'ifnull',
    'X', 1, 'VARCHAR',
    'An argument to test for NULL-ness.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'ifnull',
    'Y', 1, 'VARCHAR',
    'An argument to test for NULL-ness.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'ifnull',
    'return', 5, 'VARCHAR',
    'The first non-NULL argument.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'instr',
    'HAYSTACK', 1, 'VARCHAR',
    'The string to search.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'instr',
    'NEEDLE', 1, 'VARCHAR',
    'The string to search for within HAYSTACK.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'instr',
    'return', 5, 'INTEGER',
    'Zero if NEEDLE was not found within HAYSTACK or the index of NEEDLE within HAYSTACK plus one.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'hex',
    'X', 1, 'BLOB',
    'The blob to convert to hexadecimal',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'hex',
    'return', 5, 'VARCHAR',
    'The hexadecimal representation of the given value.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'last_insert_rowid',
    'return', 5, 'INTEGER',
    'The ROWID of the last row inserted from the current database connection.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'length',
    'X', 1, 'BLOB',
    'The value to compute the length of.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'length',
    'return', 5, 'INTEGER',
    'The total number of characters in a string or the total number of bytes in a blob.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'like',
    'PATTERN', 1, 'VARCHAR',
    'The pattern to match against the input string.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'like',
    'STR', 1, 'VARCHAR',
    'The string to test against the pattern.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'like',
    'ESCAPE', 1, 'VARCHAR',
    'The escape character.',
    3, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'like',
    'return', 5, 'BOOLEAN',
    'One if the string matches the pattern or zero if not.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'load_extension',
    'LIBRARY', 1, 'VARCHAR',
    'The name of the library to load.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'load_extension',
    'ENTRY', 1, 'VARCHAR',
    'The entry point for the SQLite extension.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'load_extension',
    'return', 5, 'NULL',
    'Always NULL.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'lower',
    'STR', 1, 'VARCHAR',
    'The string to lowercase.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'lower',
    'return', 5, 'VARCHAR',
    'The lowercased version of the input string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'ltrim',
    'STR', 1, 'VARCHAR',
    'The string to trim.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'ltrim',
    'CHARS', 1, 'VARCHAR',
    'The characters to trim from the left-side of the input string, defaults to whitespace.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'ltrim',
    'return', 5, 'VARCHAR',
    'The trimmed version of the string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'max',
    'X', 1, 'NUMERIC',
    'The first argument to test for a maximum value.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'max',
    'Y', 1, 'NUMERIC',
    'The second argument to test for a maximum value.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'max',
    'return', 5, 'NUMERIC',
    'The maximum value of those given.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'min',
    'X', 1, 'NUMERIC',
    'The first argument to test for a minimum value.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'min',
    'Y', 1, 'NUMERIC',
    'The second argument to test for a minimum value.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'min',
    'return', 5, 'NUMERIC',
    'The minimum value of those given.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'nullif',
    'X', 1, 'BLOB',
    'The first argument to test.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'nullif',
    'Y', 1, 'BLOB',
    'The second argument to test.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'nullif',
    'return', 5, 'BLOB',
    'The first argument if the arguments are different or NULL if they are different.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'printf',
    'FORMAT', 1, 'VARCHAR',
    'The format string.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'printf',
    'return', 5, 'VARCHAR',
    'The formatted string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'quote',
    'STR', 1, 'VARCHAR',
    'The string to quote.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'quote',
    'return', 5, 'VARCHAR',
    'The quoted version of the string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'random',
    'return', 5, 'INTEGER',
    'A pseudo-random integer.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'randomblob',
    'N', 1, 'INTEGER',
    'The number of bytes of random data to generate.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'randomblob',
    'return', 5, 'BLOB',
    'An N-byte blob containing pseudo-random data.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'replace',
    'HAYSTACK', 1, 'VARCHAR',
    'The string that will be searched for needles to be replaced.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'replace',
    'NEEDLE', 1, 'VARCHAR',
    'The string in the haystack to replace.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'replace',
    'REPLACEMENT', 1, 'VARCHAR',
    'The replacement text.',
    3, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'replace',
    'return', 5, 'BLOB',
    'The input string with any matches replaced.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'round',
    'X', 1, 'NUMERIC',
    'The floating-point number to round.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'round',
    'DIGITS', 1, 'INTEGER',
    'The anumber of digits to the right of the decimal point.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'round',
    'return', 5, 'NUMERIC',
    'The given floating-point number rounded to a number of digits.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'rtrim',
    'STR', 1, 'VARCHAR',
    'The string to trim.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'rtrim',
    'CHARS', 1, 'VARCHAR',
    'The characters to remove from the right side of the string.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'rtrim',
    'return', 5, 'VARCHAR',
    'The trimmed version of the string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'soundex',
    'STR', 1, 'VARCHAR',
    'The string to soundex encode.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'soundex',
    'return', 5, 'VARCHAR',
    'The soundex encoded version of the string.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'sqlite_compileoption_get',
    'N', 1, 'INTEGER',
    'The index of the compile option.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'sqlite_compileoption_get',
    'return', 5, 'VARCHAR',
    'The N-th compile-time option used to build SQLite.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'sqlite_compileoption_used',
    'NAME', 1, 'VARCHAR',
    'The name of the compile-time option to check.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'sqlite_compileoption_used',
    'return', 5, 'BOOLEAN',
    'True or false if the given option was used during compilation.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'sqlite_source_id',
    'return', 5, 'VARCHAR',
    'The SHA1 hash that uniquely identifies the source tree for this version of SQLite.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'sqlite_version',
    'return', 5, 'VARCHAR',
    'The SQLite version string.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'substr',
    'STR', 1, 'VARCHAR',
    'The input string.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'substr',
    'START', 1, 'INTEGER',
    'The starting index of the substring to return where 1 is the starting index.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'substr',
    'LENGTH', 1, 'INTEGER',
    'The number of characters in the substring.',
    3, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'substr',
    'return', 5, 'VARCHAR',
    'The substring of the input.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'total_changes',
    'return', 5, 'INTEGER',
    'The number of row changes caused by data manipulation statements since the connection was opened.',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'trim',
    'STR', 1, 'VARCHAR',
    'The string to trim.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'trim',
    'CHARS', 1, 'VARCHAR',
    'The characters to trim from the input string, defaults to whitespace.',
    2, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'trim',
    'return', 5, 'VARCHAR',
    'The trimmed version of the string.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'typeof',
    'X', 1, 'BLOB',
    'The value to return the type of.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'typeof',
    'return', 5, 'VARCHAR',
    'One of: "null", "integer", "real", "text", or "blob".',
    0, 'NO');

INSERT INTO metadata_function_arguments VALUES (
    'unicode',
    'X', 1, 'VARCHAR',
    'The string to use.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'unicode',
    'return', 5, 'VARCHAR',
    'The numeric code point for the first character in X.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'upper',
    'X', 1, 'VARCHAR',
    'The string to use.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'upper',
    'return', 5, 'VARCHAR',
    'The uppercased version of X.',
    0, 'YES');

INSERT INTO metadata_function_arguments VALUES (
    'zeroblob',
    'N', 1, 'INTEGER',
    'The size of the blob to create.',
    1, 'YES');
INSERT INTO metadata_function_arguments VALUES (
    'zeroblob',
    'return', 5, 'BLOB',
    'A blob of size N.',
    0, 'NO');
