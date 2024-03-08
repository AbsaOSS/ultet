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
import za.co.absa.ultet.types.user.UserName
import za.co.absa.ultet.util.{FileReader, ResourceReader}

import java.nio.file.Paths

class DBAppModelTest_SchemaOwners extends AnyFunSuiteLike {

  private def schema(schemaName: SchemaName, ownerName: UserName): SchemaDef = {
    SchemaDef(
      name = schemaName,
      ownerName =  Some(ownerName),
      functions = Set.empty,
      tablesFromSource = Map.empty,
      tablesFromTarget = Map.empty
    )
  }

  private def schema(schemaName: SchemaName): SchemaDef = {
    SchemaDef(
      name = schemaName,
      ownerName = None,
      functions = Set.empty,
      tablesFromSource = Map.empty,
      tablesFromTarget = Map.empty
    )
  }

  test("Adding schema owner") {
    val schemaOwner = UserName("user1")
    val db1Name = DatabaseName("db1")
    val db2Name = DatabaseName("db2")
    val db3Name = DatabaseName("db3")
    val schema1Name = SchemaName("foo")
    val schema2Name = SchemaName("bar")

    val initAppModel = DBAppModel(
      Map(
        db1Name -> DatabaseDef(
          databaseName = db1Name,
          schemas = Map(
            schema1Name -> schema(schema1Name)
          ),
          createDatabase = false
        ),
        db2Name -> DatabaseDef(
          databaseName = db2Name,
          schemas = Map(
            schema1Name -> schema(schema1Name),
            schema2Name -> schema(schema2Name)
          ),
          createDatabase = false
        ),
        db3Name -> DatabaseDef(
          databaseName = db3Name,
          schemas = Map(),
          createDatabase = false
        )
      ),
      Map.empty
    )

    val expected = DBAppModel(
      Map(
        db1Name -> DatabaseDef(
          databaseName = db1Name,
          schemas = Map(
            schema1Name -> schema(schema1Name, schemaOwner)
          ),
          createDatabase = false
        ),
        db2Name -> DatabaseDef(
          databaseName = db2Name,
          schemas = Map(
            schema1Name -> schema(schema1Name, schemaOwner),
            schema2Name -> schema(schema2Name)
          ),
          createDatabase = false
        ),
        db3Name -> DatabaseDef(
          databaseName = db3Name,
          schemas = Map(),
          createDatabase = false
        )
      ),
      Map(schema1Name -> schemaOwner)
    )

    val result = initAppModel.addSchemaOwner(schema1Name, schemaOwner)

    assert(result == expected)
  }

  test("Existing schema owner is assigned when schema is created") {
    val functionOwner = UserName("user1")
    val tableOwner = UserName("user2")
    val fooOwner = UserName("user3")
    val db1Name = DatabaseName("example_db")
    val schema1Name = SchemaName("my_schema")
    val schema2Name = SchemaName("other_schema")
    val schema3Name = SchemaName("foo")

    val initAppModel = DBAppModel(
      Map(
        db1Name -> DatabaseDef(
          databaseName = db1Name,
          schemas = Map(),
          createDatabase = false
        )
      ),
      Map(
        schema1Name -> functionOwner,
        schema2Name -> tableOwner,
        schema3Name -> fooOwner
      )
    )

    val expectedTable = Examples.myTableInSchema(schema2Name)
    val expected = DBAppModel(
      Map(
        db1Name -> DatabaseDef(
          databaseName = db1Name,
          schemas = Map(
            schema1Name -> schema(schema1Name, functionOwner).copy(functions = Set(Examples.publicFunction)),
            schema2Name -> schema(schema2Name, tableOwner).copy(tablesFromSource = Map(expectedTable.tableName->expectedTable)),
          ),
          createDatabase = false
        )
      ),
      Map(
        schema1Name -> functionOwner,
        schema2Name -> tableOwner,
        schema3Name -> fooOwner
      )
    )

    //println(Paths.get("examples/database/src/main/my_schema/public_function.sql").toUri)
    val functionsSrc = FileReader.readFileAsString(Paths.get("examples/database/src/main/my_schema/public_function.sql").toUri)
    val tableSrc = FileReader.readFileAsString(Paths.get("examples/database/src/main/my_schema/my_table.yaml").toUri)
    val intermediate = initAppModel.addFunctionSource(schema1Name, functionsSrc)
    val result = intermediate.addTableSource(schema2Name, tableSrc)

    assert(result == expected)

  }

  test("Merge of schema owners when two DAAppModels are merged") {
    val dbName = DatabaseName("example_db")
    val schema1Name = SchemaName("foo")
    val schema2Name = SchemaName("bar")
    val schema3Name = SchemaName("baz")

    val user1 = UserName("user1")
    val user2 = UserName("user2")


    val firstAppModel = DBAppModel(
      Map(
        dbName -> DatabaseDef(
          databaseName = dbName,
          schemas = Map(
            schema1Name -> schema(schema1Name, user1),
            schema2Name -> schema(schema2Name, user1),
            schema3Name -> schema(schema3Name)
          ),
          createDatabase = false
        )
      ),
      Map(
        schema1Name -> user1,
        schema2Name -> user1
      )
    )
    val secondAppModel = DBAppModel(
      Map(
        dbName -> DatabaseDef(
          databaseName = dbName,
          schemas = Map(
            schema1Name -> schema(schema1Name),
            schema2Name -> schema(schema2Name, user2),
            schema3Name -> schema(schema3Name, user2)
          ),
          createDatabase = false
        )
      ),
      Map(
        schema2Name -> user2,
        schema3Name -> user2
      )
    )

    val expected = DBAppModel(
      Map(
        dbName -> DatabaseDef(
          databaseName = dbName,
          schemas = Map(
            schema1Name -> schema(schema1Name, user1),
            schema2Name -> schema(schema2Name, user1),
            schema3Name -> schema(schema3Name, user2)
          ),
          createDatabase = false
        )
      ),
      Map(
        schema1Name -> user1,
        schema2Name -> user1,
        schema3Name -> user2
      )
    )

    val result = firstAppModel + secondAppModel

    assert(result == expected)
  }

}
