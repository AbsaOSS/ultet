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

import za.co.absa.ultet.dbitems.{DBFunction, DBFunctionFromSource, DBItem, DBSchema}
import za.co.absa.ultet.model.function.FunctionName
import za.co.absa.ultet.util.Tables

case class SchemaDef(
                      name: SchemaName,
                      functions: Set[DBFunction],
                      tablesFromSource: Tables,
                      tablesFromTarget: Tables
                    ) {

  def +(other: Option[SchemaDef]): SchemaDef = {
    other.fold(this)(this + _)
  }
  def +(other: SchemaDef): SchemaDef = {
    SchemaDef(
      name,
      functions ++ other.functions,
      tablesFromSource ++ other.tablesFromSource,
      tablesFromTarget ++ other.tablesFromTarget
    )
  }

  def functionNames: Set[FunctionName] = {
    functions.map(_.fnName)
  }

  def users: Set[UserName] = {
    val functionUsers = functions
      .collect{case function: DBFunctionFromSource => (function.users, function.owner)}
      .foldLeft(Set.empty[UserName]){ case (acc, (users, owner)) =>
        acc ++ users + owner
      }
    tablesFromSource.values.foldLeft(functionUsers) { case (acc, table) =>
      acc + table.owner
    }
  }

  def toDBItems: Set[DBItem] = {
    val accWithSchema: Seq[DBItem] = Seq(DBSchema(name, None /*TODO #30 Schema owner support  */, users))
    val accWithTables = tablesFromSource.foldLeft(accWithSchema) { case (acc, (tableName, tableDef)) =>
      acc :+ (tableDef - tablesFromTarget.get(tableName))
    }
    functions ++ accWithTables
  }

}
