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

package za.co.absa.ultet.implicits

import org.scalatest.funsuite.{AnyFunSuite, AnyFunSuiteLike}
import za.co.absa.ultet.util.SqlEntriesPerTransaction
import za.co.absa.ultet.implicits.MapImplicits.SqlEntriesPerTransactionEnhancement
import za.co.absa.ultet.model.function.{FunctionArgumentType, FunctionDrop, FunctionName}
import za.co.absa.ultet.model.{SchemaName, TransactionGroup, UserEntry, UserName}

class MapImplicits_SqlEntriesPerTransactionEnhancementTest extends AnyFunSuiteLike {
  test("toSql without any entries") {
    val input:SqlEntriesPerTransaction = Map.empty
    val result = input.toSql
    assert(result.isEmpty)
  }

  test("toSql wit two transaction groups") {

    val transaction1 = TransactionGroup.Roles
    val transaction2 = TransactionGroup.Objects
    val entry1 = UserEntry(UserName("user1"))
    val entry2 = FunctionDrop(SchemaName("foo"), FunctionName("fmc1"), Seq.empty)
    val entry3 = FunctionDrop(SchemaName("bar"), FunctionName("fnc2"), Seq(FunctionArgumentType("TEXT")))
    val input: SqlEntriesPerTransaction = Map(
      transaction1 -> Seq(entry1),
      transaction2 -> Seq(entry2, entry3)
    )
    val result = input.toSql

    val expected1 = Seq(
      """
        |----------------------------------------------------------------------------------------
        |-- Transaction group: ROLES
        |----------------------------------------------------------------------------------------
        |""".stripMargin,
      """DO
        |$do$
        |  BEGIN
        |    IF EXISTS (
        |      SELECT FROM pg_catalog.pg_roles
        |      WHERE lowercase(rolname) = 'user1') THEN
        |
        |      RAISE NOTICE 'Role "user1" already exists. Skipping.';
        |    ELSE
        |      CREATE ROLE user1;
        |    END IF;
        |  END
        |$do$;""".stripMargin
    )
    val expected2 = Seq(
      """
        |----------------------------------------------------------------------------------------
        |-- Transaction group: OBJECTS
        |----------------------------------------------------------------------------------------
        |""".stripMargin,
      """DROP FUNCTION foo.fmc1();""",
      """DROP FUNCTION bar.fnc2(TEXT);"""
    )

    assert(result.values.toList == List(expected1, expected2))
  }
}
