SELECT N.nspname AS schema_name,
       T.relname AS table_name,
       pg_get_userbyid(T.relowner) AS table_owner,
       T.relhasindex AS has_indexes,
       D.description
FROM pg_class T
         INNER JOIN pg_namespace N ON N.oid = T.relnamespace
         LEFT JOIN pg_description D ON D.objoid = T.oid AND D.objsubid = 0
WHERE
    T.relkind = ANY (ARRAY['r'::"char", 'p'::"char"])
    AND N.nspname = ?
    AND T.relname = ?