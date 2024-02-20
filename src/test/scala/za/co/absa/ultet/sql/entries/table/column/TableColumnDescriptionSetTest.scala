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

package za.co.absa.ultet.sql.entries.table.column

import org.scalatest.funsuite.AnyFunSuiteLike
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{ColumnName, TableIdentifier, TableName}

class TableColumnDescriptionSetTest extends AnyFunSuiteLike {
  private val tableIdentifier = TableIdentifier(SchemaName("foo"), TableName("bar"))
  private val columnName = ColumnName("baz")

  test("Setting a table column comment") {
    val expected = "COMMENT ON COLUMN foo.bar.baz\nIS 'This is a comment';"

    val result = TableColumnDescriptionSet(tableIdentifier, columnName, Some("This is a comment")).sqlExpression
    assert(result == expected)
  }

  test("Removing a table column comment") {
    val expected = "COMMENT ON COLUMN foo.bar.baz\nIS NULL;"

    val result = TableColumnDescriptionSet(tableIdentifier, columnName, None).sqlExpression
    assert(result == expected)
  }

}
