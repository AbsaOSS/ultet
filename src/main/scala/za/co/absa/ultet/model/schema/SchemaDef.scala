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

package za.co.absa.ultet.model.schema

import za.co.absa.ultet.model.function.{FunctionFromSource, FunctionHeader}
import za.co.absa.ultet.model.schema.SchemaDef.{DO_NOT_CHOWN, DO_NOT_TOUCH}
import za.co.absa.ultet.model.DBItem
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.sql.entries.schema.{SchemaCreation, SchemaGrant, SchemaOwnership}
import za.co.absa.ultet.types.complex.Tables
import za.co.absa.ultet.types.function.FunctionName
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.user.UserName

case class SchemaDef(
                      name: SchemaName,
                      ownerName: Option[UserName],
                      functions: Set[FunctionHeader],
                      tablesFromSource: Tables,
                      tablesFromTarget: Tables
                    ) extends DBItem {

  override def sqlEntries: Seq[SQLEntry] = {
    if (DO_NOT_TOUCH.contains(name.value)) {
      throw new Exception(s"Schema ${name.value} is not allow to be referenced") //TODO  #31 Add warnings to the system
    }

    if (DO_NOT_CHOWN.contains(name.value)) {
      throw new Exception(s"Schema ${name.value} is not allowed to change owner")
// TODO  #31 Add warnings to the system
//      Seq(
//        SchemaCreation(name),
//        SchemaGrant(name, users)
//      )
    } else {
      ownerName.map {
        SchemaOwnership(name, _)
      }.toSeq ++
        Seq(
          SchemaCreation(name),
          SchemaGrant(name, users)
        )
    }
  }

  def +(other: Option[SchemaDef]): SchemaDef = {
    other.fold(this)(this + _)
  }
  def +(other: SchemaDef): SchemaDef = {
    SchemaDef(
      name,
      ownerName.orElse(other.ownerName),
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
      .collect{case function: FunctionFromSource => (function.users, function.owner)}
      .foldLeft(Set.empty[UserName]){ case (acc, (users, owner)) =>
        acc ++ users + owner
      }
    tablesFromSource.values.foldLeft(functionUsers) { case (acc, table) =>
      acc + table.owner
    }
  }

  def toDBItems: Set[DBItem] = {
    val accWithSchema: Seq[DBItem] = Seq(this)
    val accWithTables = tablesFromSource.foldLeft(accWithSchema) { case (acc, (tableName, tableDef)) =>
      acc :+ (tableDef - tablesFromTarget.get(tableName))
    }
    functions ++ accWithTables
  }

}

object SchemaDef {

  val DO_NOT_TOUCH: Seq[String] = Seq("pg_toast", "pg_catalog", "information_schema") // some schemas (system ones) are not to be touched
  val DO_NOT_CHOWN: Seq[String] = Seq("public") // some schemas should not change their owner

}
