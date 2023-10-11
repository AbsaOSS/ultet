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
    val schemas = filePathsPerSchema.keys
    val SchemasWithFilesGroupedByType(tables, functions, owners) = groupAllFilesPerSchemaByType(filePathsPerSchema)

    // TODO handle tables

    val dbFunctionsFromSource = functions
      .values
      .flatten
      .map(PgFunctionFileParser().parseFile)
      .toSet
    val dbFunctionsFromPG = functions
      .keys
      .flatMap(DBFunctionFromPG.fetchAllOfSchema)
      .toSet

    val users: Set[DBItem] = dbFunctionsFromSource.flatMap(f => f.users :+ f.owner).map(DBUser)

    val schemaOwners = owners.mapValues(DBSchema.parseTxtFileContainingSchemaOwner)
    val schemaUsers = dbFunctionsFromSource.groupBy(_.schema).mapValues(_.toSeq.map(_.users).flatten.toSet)

    val dbSchemas = schemas.toSet.map { (s: SchemaName) =>
      val owner = schemaOwners(s)
      val users = schemaUsers(s) + owner
      DBSchema(s, owner, users.toSeq)
    }

    dbFunctionsFromSource ++ dbFunctionsFromPG ++ users ++ dbSchemas
  }

  private case class SchemasWithFilesGroupedByType(
                                                    tables: Map[SchemaName, Seq[URI]],
                                                    functions: Map[SchemaName, Seq[URI]],
                                                    owners: Map[SchemaName, URI]
                                                  )

  private def groupAllFilesPerSchemaByType(all: Map[SchemaName, Seq[URI]]): SchemasWithFilesGroupedByType = {
    val tableFiles = all.mapValues(_.filter(_.getPath.endsWith(".yml")))
    val functionFiles = all.mapValues(_.filter(_.getPath.endsWith(".sql")))
    val ownerFiles = all.mapValues(_.filter(_.getPath.endsWith(".txt")))
    ownerFiles.foreach { case (schemaName, uris) =>
      if (uris.size > 1) throw new IllegalArgumentException(
        s"Detected more than one .txt file in schema ${schemaName.normalized}"
      ) else if (uris.size == 0) throw new IllegalArgumentException(
        s".txt file in schema ${schemaName.normalized} not found"
      )
    }
    val ownerFilePerSchema = ownerFiles.mapValues(_.head)

    SchemasWithFilesGroupedByType(tableFiles, functionFiles, ownerFilePerSchema)
  }

}
