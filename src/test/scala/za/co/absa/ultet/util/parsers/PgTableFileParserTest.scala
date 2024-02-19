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

package za.co.absa.ultet.util.parsers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.model.table.index.TableIndex.{PrimaryKey, SecondaryIndex, IndexColumn}
import za.co.absa.ultet.model.table.column.TableColumn
import za.co.absa.ultet.model.table.TableDef
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{ColumnName, IndexName, TableIdentifier, TableName}
import za.co.absa.ultet.types.user.UserName
import za.co.absa.ultet.util.parsers.helpers.DBTableFromYaml

class PgTableFileParserTest extends AnyFlatSpec with Matchers {

  private val schemaName = SchemaName("testSchema")

  "PgTableFileParserTest" should "return semi-prepared object table from example content" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns:
        |  - columnName: column1
        |    dataType: bigint
        |    notNull: "true"
        |primaryKey:
        |    name: pk_my_table
        |    columns: "[id_key_field1, id_key_field1]"
        |indexes:
        |  - indexName: idx_some_name
        |    tableName: testTable
        |    indexBy: "[column1]"
        |""".stripMargin

    PgTableFileParser(schemaName).processYaml(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
      description = Some("Some Description of this madness"),
      primaryDBName = "primaryDB",
      owner = "some_owner_user",
      columns = Seq(
        Map(
          "columnName" -> "column1",
          "dataType" -> "bigint",
          "notNull" -> "true"  // TODO, horrible! Must be string in the YAML file!
        ),
      ),
      primaryKey = Some(Map(
        "name" -> "pk_my_table",
        "columns" -> "[id_key_field1, id_key_field1]"  // TODO, horrible! Must be string in the YAML file!
      )),
      indexes = Seq(Map(
        "indexName" -> "idx_some_name",
        "tableName" -> "testTable",
        "indexBy" -> "[column1]"  // TODO, horrible! Must be string in the YAML file!
      ))
    )
  }

  "PgTableFileParserTest" should "return well-prepared table object from example content" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns:
        |  - columnName: column1
        |    dataType: bigint
        |    notNull: "true"
        |primaryKey:
        |    name: pk_my_table
        |    columns: "[id_key_field1, id_key_field1]"
        |indexes:
        |  - indexName: idx_some_name
        |    tableName: testTable
        |    indexBy: "[column1]"
        |""".stripMargin

    PgTableFileParser(schemaName).parseSource(tableString).head shouldBe TableDef(
      tableName = TableName("testTable"),
      schemaName = SchemaName("testSchema"),
      description = Some("Some Description of this madness"),
      primaryDBName = DatabaseName("primaryDB"),
      owner = UserName("some_owner_user"),
      columns = Seq(
        TableColumn(
          columnName = ColumnName("column1"),
          dataType = "bigint",
          notNull = true
        ),
      ),
      primaryKey = Some(PrimaryKey(
        tableIdentifier = TableIdentifier(SchemaName("testSchema"), TableName("testTable")),
        columns = Seq("id_key_field1", "id_key_field1").map(IndexColumn(_)),
        indexName = IndexName("pk_my_table")
      )),
      indexes = Set(SecondaryIndex(
        tableIdentifier = TableIdentifier(SchemaName("testSchema"), TableName("testTable")),
        indexName = IndexName("idx_some_name"),
        columns = Seq("column1").map(IndexColumn(_))
      ))
    )
  }

  "PgTableFileParserTest" should "return semi-prepared object table from example content, some attributes empty" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns: []
        |primaryKey:
        |indexes: []
        |""".stripMargin

    PgTableFileParser(schemaName).processYaml(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
      description = Some("Some Description of this madness"),
      primaryDBName = "primaryDB",
      owner = "some_owner_user",
    )
  }

  "PgTableFileParserTest" should "return well-prepared object table from example content, some attributes empty" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns: []
        |primaryKey:
        |indexes: []
        |""".stripMargin

    PgTableFileParser(schemaName).parseSource(tableString).head shouldBe TableDef(
      tableName = TableName("testTable"),
      schemaName = SchemaName("testSchema"),
      description = Some("Some Description of this madness"),
      primaryDBName = DatabaseName("primaryDB"),
      owner = UserName("some_owner_user"),
    )
  }
}
