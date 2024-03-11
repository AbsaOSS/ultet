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

package za.co.absa.ultet.model

import org.scalatest.funsuite.AnyFunSuiteLike
import za.co.absa.ultet.Examples
import za.co.absa.ultet.model.schema.SchemaDef
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.util.FileReader

class DBAppModelTest  extends AnyFunSuiteLike {
  test("Loading examples") {
    val expectedDatabaseName = DatabaseName("example_db")
    val expectedSchemaName = SchemaName("my_schema")
    val expectedModel = DBAppModel(Map(
      expectedDatabaseName -> DatabaseDef(
        databaseName = expectedDatabaseName,
        schemas = Map(
          expectedSchemaName -> SchemaDef(
            name = expectedSchemaName,
            ownerName = None,
            functions = Set(Examples.publicFunction, Examples.internalFunction),
            tablesFromSource = Map(Examples.myTable.tableName -> Examples.myTable),
            tablesFromTarget = Map.empty
          )
        ),
        createDatabase = false
      )
    ), Map.empty)

    val files = FileReader.listFileURIsPerSchema("examples/database/src/main")
    val resultModel = DBAppModel.loadFromSources(files)

    assert(resultModel == expectedModel)
  }
}
