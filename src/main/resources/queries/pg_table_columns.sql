SELECT
    N.nspname AS schema_name,
    T.relname AS table_name,
    A.attname AS column_name,
    C.ordinal_position,
    pg_catalog.format_type (a.atttypid,a.atttypmod) AS data_type,
    NOT A.attnotnull AS is_nullable,
    C.column_default,
    D.description
FROM
    pg_class T
        INNER JOIN pg_namespace N ON N.oid = T.relnamespace
        INNER JOIN pg_attribute A ON A.attrelid = T.Oid
        INNER JOIN information_schema.columns C ON
                C.table_schema = N.nspname AND
                C.table_name = T.relname AND
                C.column_name = A.attname
        LEFT JOIN pg_description D ON
                D.objoid = T.oid AND
                D.objsubid = C.ordinal_position
WHERE
    T.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])
    AND N.nspname = ?
    AND T.relname = ?
ORDER BY
    C.ordinal_position