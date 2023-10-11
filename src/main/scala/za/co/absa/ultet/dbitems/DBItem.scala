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

package za.co.absa.ultet.dbitems

import za.co.absa.ultet.model.{SQLEntry, SchemaName}
import za.co.absa.ultet.parsers.PgFunctionFileParser

import java.net.URI
import java.sql.Connection

trait DBItem {
  def sqlEntries: Seq[SQLEntry]
}

object DBItem {

  def createDBItems(filePathsPerSchema: Map[SchemaName, Seq[URI]])
                   (implicit jdbcConnection: Connection): Set[DBItem] = {
    val tableFiles = filePathsPerSchema.mapValues(_.filter(_.getPath.endsWith(".yml")))
    val functionFiles = filePathsPerSchema.mapValues(_.filter(_.getPath.endsWith(".sql")))

    // TODO handle tables

    val dbFunctionsFromSource: Set[DBFunctionFromSource] = functionFiles
      .values
      .flatten
      .map(PgFunctionFileParser().parseFile)
      .toSet
    val dbFunctionsFromPG: Set[DBFunctionFromPG] = functionFiles
      .keys
      .map(DBFunctionFromPG.fetchAllOfSchema)
      .flatten
      .toSet

    val users: Set[DBItem] = dbFunctionsFromSource.map(f => f.users ++ Seq(f.owner)).flatten.map(DBUser)

    dbFunctionsFromSource ++ dbFunctionsFromPG ++ users
  }

}
