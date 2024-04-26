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

package za.co.absa.ultet

import za.co.absa.ultet.model.function.FunctionFromSource
import za.co.absa.ultet.model.table.TableDef
import za.co.absa.ultet.model.table.column.TableColumn
import za.co.absa.ultet.model.table.index.TableIndex.{IndexColumn, PrimaryKey, SecondaryIndex}
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.function.{FunctionArgumentType, FunctionName}
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{ColumnName, IndexName, TableIdentifier, TableName}
import za.co.absa.ultet.types.user.UserName
import za.co.absa.ultet.util.FileReader

import java.nio.file.Paths

/**
 * This object contains the model instances of the filesystem
 */
object Examples {

  val internalFunction: FunctionFromSource = FunctionFromSource(
    fnName = FunctionName("_internal_function"),
    paramTypes = Seq(FunctionArgumentType("DATE")),
    owner = UserName("some_owner_user"),
    users = Set.empty,
    schema = SchemaName("my_schema"),
    database = DatabaseName("example_db"),
    sqlBody = FileReader.readFileAsString(Paths.get("examples/database/src/main/my_schema/_internal_function.sql").toUri)
  )

  val publicFunction: FunctionFromSource = FunctionFromSource(
    fnName = FunctionName("public_function"),
    paramTypes = Seq(FunctionArgumentType("TEXT")),
    owner = UserName("some_owner_user"),
    users = Set(UserName("user_for_access")),
    schema = SchemaName("my_schema"),
    database = DatabaseName("example_db"),
    sqlBody = FileReader.readFileAsString(Paths.get("examples/database/src/main/my_schema/public_function.sql").toUri)
  )

  val myTable: TableDef = myTableInSchema(SchemaName("my_schema"))

  /**
   * This functions places the example table into the specified schema
   *
   * @param schemaName The name of the schema to placed the table in
   * @return The table definition
   */
  def myTableInSchema(schemaName: SchemaName): TableDef = {
    val tableIdentifier = TableIdentifier(schemaName, TableName("my_table"))

    TableDef(
      tableIdentifier = tableIdentifier,
      primaryDBName = DatabaseName("example_db"),
      owner = UserName("some_owner_user"),
      description = Some("This is an example table"),
      columns = Seq(
        TableColumn(ColumnName("id_key_field"), "bigint", notNull = true, Some("Key field"), Some("SQL expression")),
        TableColumn(ColumnName("some_name"), "text", notNull = false, Some("Aggregation name")),
        TableColumn(ColumnName("item_tags"), "text[]", notNull = true, Some("Array of values"))
      ),
      primaryKey = Some(PrimaryKey(
        tableIdentifier = tableIdentifier,
        indexName = IndexName("pk_my_table"),
        columns = Seq(IndexColumn("id_key_field"))
      )),
      indexes = Set(
        SecondaryIndex(
          tableIdentifier = tableIdentifier,
          indexName = IndexName("idx_some_name"),
          columns = Seq(IndexColumn("column1"))
        )
      )
    )
  }

}
