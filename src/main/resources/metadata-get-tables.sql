SELECT ? as TABLE_CAT,
    null as TABLE_SCHEM,
    name as TABLE_NAME,
    upper(type) as TABLE_TYPE,
    sql as REMARKS,
    null as TYPE_CAT,
    null as TYPE_SCHEM,
    null as TYPE_NAME,
    "row_id" as SELF_REFERENCING_COL_NAME,
    "SYSTEM" as REF_GENERATION FROM %Q.sqlite_master
    WHERE name LIKE ? and upper(type) in (%s)
    ORDER BY TABLE_TYPE,
    TABLE_CAT,
    TABLE_SCHEM,
    TABLE_NAME
