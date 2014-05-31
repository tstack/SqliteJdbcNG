SELECT null AS FUNCTION_CAT,
    null AS FUNCTION_SCHEM,
    FUNCTION_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,
    metadata_types.DATA_TYPE,
    metadata_types.TYPE_NAME,
    metadata_types.PRECISION,
    0 AS LENGTH,
    metadata_types.MAXIMUM_SCALE AS SCALE,
    metadata_types.NUM_PREC_RADIX AS RADIX,
    CASE IS_NULLABLE
        WHEN 'YES' THEN 1
        WHEN 'NO' THEN 0
        WHEN '' THEN 2
    END AS NULLABLE,
    REMARKS,
    CASE metadata_types.TYPE_NAME
        WHEN 'TEXT' THEN metadata_types.PRECISION
        ELSE null
    END AS CHAR_OCTET_LENGTH,
    ORDINAL_POSITION,
    IS_NULLABLE,
    null AS SPECIFIC_NAME
    FROM metadata_function_arguments
    LEFT JOIN metadata_types ON metadata_function_arguments.TYPE_NAME = metadata_types.TYPE_NAME
    WHERE FUNCTION_NAME LIKE ? AND COLUMN_NAME LIKE ?
    ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME
