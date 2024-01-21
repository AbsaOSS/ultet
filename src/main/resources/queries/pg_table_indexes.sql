SELECT
    N.nspname AS schema_name,
    T.relname AS table_name,
    I.indexrelid::regclass AS index_name,
    AM.amname AS index_type,
    D.description,
    pg_get_expr(I.indpred, I.indrelid) AS constraint_expression,
    I.indisprimary AS is_primary,
    I.indisunique AS is_unique,
    NOT(I.indnullsnotdistinct) AS nulls_distinct,
    pg_get_indexdef(A.attrelid, A.attnum, true) AS column_expression,
    pg_index_column_has_property(I.indexrelid, A.attnum, 'ASC') AS is_ascending,
    pg_index_column_has_property(I.indexrelid, A.attnum, 'nulls_first') AS nulls_first
FROM
    pg_namespace N
        INNER JOIN pg_class T ON T.relnamespace = N.oid
        INNER JOIN pg_index I ON I.indrelid = T.oid
        INNER JOIN pg_attribute A ON A.attrelid = I.indexrelid
        INNER JOIN pg_class C ON C.oid = I.indexrelid
        INNER JOIN pg_am AM ON AM.oid = C.relam
        LEFT JOIN pg_description D ON I.indexrelid = D.objoid
WHERE
    T.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])
    AND N.nspname = ?
    AND T.relname = ?
ORDER BY index_name, A.attnum