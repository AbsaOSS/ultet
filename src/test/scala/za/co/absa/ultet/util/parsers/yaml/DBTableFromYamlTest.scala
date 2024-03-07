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

package za.co.absa.ultet.util.parsers.yaml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.types.schema.SchemaName

class DBTableFromYamlTest extends AnyFlatSpec with Matchers {

  private val schemaName = SchemaName("testSchema")

  "PgTableFileParserTest" should "return parse yaml from example content" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primary_db: primaryDB
        |owner: some_owner_user
        |columns:
        |  - column_name: column1
        |    data_type: bigint
        |    not_null: true
        |primary_key:
        |    name: pk_my_table
        |    columns: [id_key_field1, id_key_field1]
        |indexes:
        |  - index_name: idx_some_name
        |    index_by: [column1]
        |  - index_name: idx_some_name2
        |    index_by: [column1]
        |    is_unique: true
        |
        |""".stripMargin

    DBTableFromYaml.fromYamlSource(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
      description = Some("Some Description of this madness"),
      primaryDb = "primaryDB",
      owner = "some_owner_user",
      columns = Seq(
        ColumnFromYaml(
          columnName = "column1",
          dataType = "bigint",
          notNull = true
        ),
      ),
      primaryKey = Some(PrimaryKeyFromYaml(
        name = Some("pk_my_table"),
        columns = Seq("id_key_field1", "id_key_field1")
      )),
      indexes = Seq(
        SecondaryIndexFromYaml("idx_some_name", Seq("column1")),
        SecondaryIndexFromYaml("idx_some_name2", Seq("column1"), isUnique = true)
      )
    )
  }

  "PgTableFileParserTest" should "return parse yaml from example content, some attributes empty" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primary_db: primaryDB
        |owner: some_owner_user
        |columns: []
        |primary_key:
        |indexes: []
        |""".stripMargin

    DBTableFromYaml.fromYamlSource(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
      description = Some("Some Description of this madness"),
      primaryDb = "primaryDB",
      owner = "some_owner_user",
    )
  }

}
