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

package za.co.absa.ultet.sql.entries.table

import org.scalatest.funsuite.AnyFunSuiteLike
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{TableIdentifier, TableName}

class TableDescriptionSetTest extends AnyFunSuiteLike {
  private val tableIdentifier = TableIdentifier(SchemaName("foo"), TableName("bar"))

  test("Setting a table comment") {
    val expected = "COMMENT ON TABLE foo.bar IS 'This is a comment';"

    val result = TableDescriptionSet(tableIdentifier,Some("This is a comment")).sqlExpression
    assert(result == expected)
  }

  test("Removing a table comment") {
    val expected = "COMMENT ON TABLE foo.bar IS NULL;"

    val result = TableDescriptionSet(tableIdentifier, None).sqlExpression
    assert(result == expected)
  }
}
