/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-- owner: some_owner_user, other_owner
-- database: example_db ( user_for_access )
CREATE OR REPLACE FUNCTION my_schema.public_function(
    IN  i_parameter         TEXT,
    OUT status              INTEGER,
    OUT status_text         TEXT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: my_schema.public_function([Function_Param_Count])
--      [Description]
--
-- Parameters:
--      i_parameter         -
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--
-- Status codes:
--      10                  - OK
--
-------------------------------------------------------------------------------
DECLARE
BEGIN

END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;
