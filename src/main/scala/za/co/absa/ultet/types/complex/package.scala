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

package za.co.absa.ultet.types

import za.co.absa.ultet.model.DatabaseDef
import za.co.absa.ultet.model.schema.SchemaDef
import za.co.absa.ultet.model.table.TableDef
import za.co.absa.ultet.sql.TransactionGroup
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.TableName
import za.co.absa.ultet.types.user.UserName

package object complex {

  type DatabaseDefs = Map[DatabaseName, DatabaseDef]

  type SchemaDefs = Map[SchemaName, SchemaDef]

  type SqlEntriesPerTransaction = Map[TransactionGroup.Value, Seq[SQLEntry]]

  type SqlsPerTransaction = Map[TransactionGroup.Value, Seq[String]]

  type Tables = Map[TableName, TableDef]

  type SchemaOwners = Map[SchemaName, UserName]

}
