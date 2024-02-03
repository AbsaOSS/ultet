/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * A query that returns all indexes, including the primary index, of a table chosen by its name and schema.
 */
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
ORDER BY index_name, A.attnum;
