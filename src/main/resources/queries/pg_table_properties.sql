/*
 * Copyright 2023 ABSA Group Limited
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
 * A query that returns properties of a table chosen by its name and schema.
 */
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
    AND T.relname = ?;
