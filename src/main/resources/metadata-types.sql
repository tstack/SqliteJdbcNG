CREATE TABLE metadata_types (
    TYPE_NAME VARCHAR(64) PRIMARY KEY,
    DATA_TYPE INT,
    PRECISION INT,
    LITERAL_PREFIX VARCHAR(64),
    LITERAL_SUFFIX VARCHAR(64),
    CREATE_PARAMS VARCHAR(64),
    NULLABLE SMALLINT,
    CASE_SENSITIVE SMALLINT,
    SEARCHABLE SMALLINT,
    UNSIGNED_ATTRIBUTE SMALLINT,
    FIXED_PREC_SCALE SMALLINT,
    AUTO_INCREMENT SMALLINT,
    LOCAL_TYPE_NAME VARCHAR(64),
    MINIMUM_SCALE SMALLINT,
    MAXIMUM_SCALE SMALLINT,
    SQL_DATA_TYPE SMALLINT,
    SQL_DATETIME_SUB SMALLINT,
    NUM_PREC_RADIX INT
);

INSERT INTO metadata_types VALUES (
    'TEXT', -1, %1$d, '''', '''', null, 1, 1, 3, 0, 0, 0, null, 0, 0, null, null, null
);

INSERT INTO metadata_types VALUES (
    'NULL', 0, null, null, null, null, 1, 0, 2, 0, 0, 0, null, 0, 0, null, null, null
);

INSERT INTO metadata_types VALUES (
    'NUMERIC', 2, 16, null, null, null, 1, 0, 2, 0, 0, 0, null, 0, 14, null, null, 10
);

INSERT INTO metadata_types VALUES (
    'INTEGER', 4, 19, null, null, null, 1, 0, 2, 0, 0, 1, null, 0, 0, null, null, 10
);

INSERT INTO metadata_types VALUES (
    'REAL', 7, 16, null, null, null, 1, 0, 2, 0, 0, 0, null, 0, 14, null, null, 10
);

INSERT INTO metadata_types VALUES (
    'VARCHAR', 12, %1$d, '''', '''', null, 1, 1, 3, 0, 0, 0, null, 0, 0, null, null, null
);

INSERT INTO metadata_types VALUES (
    'BOOLEAN', 16, 1, null, null, null, 1, 0, 2, 0, 0, 0, null, 0, 0, null, null, 2
);

INSERT INTO metadata_types VALUES (
    'BLOB', 2004, %1$d, '''', '''', null, 1, 1, 3, 0, 0, 0, null, 0, 0, null, null, null
);
