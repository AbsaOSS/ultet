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

import za.co.absa.ultet.dbitems.DBItem
import za.co.absa.ultet.util.SchemaDefs

case class DatabaseDef(
                        databaseName: DatabaseName,
                        schemas: SchemaDefs,
                        createDatabase: Boolean
                      ) {
  def +(other: Option[DatabaseDef]): DatabaseDef = {
    other.fold(this)(this + _)
  }

  def +(other: DatabaseDef): DatabaseDef = {
    val newSchemas = other.schemas.foldLeft(schemas) {
      case (acc, (schemaName, schemaDef)) =>
        val joinedSchema = schemaDef + schemas.get(schemaName)
        acc + (schemaName -> joinedSchema)
    }

    DatabaseDef(
      databaseName,
      newSchemas,
      createDatabase || other.createDatabase
    )
  }

  def toDBItems: Set[DBItem] = {
    // TODo #32 Add database creation support
    schemas.values.foldLeft(Set.empty[DBItem])((acc, schemaDef) => schemaDef.toDBItems ++ acc)
  }
}
