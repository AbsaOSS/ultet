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

import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.types.schema.SchemaName

import java.net.URI

trait DBItem {
  def sqlEntries: Seq[SQLEntry]
}

object DBItem {

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
      ) else if (uris.isEmpty) throw new IllegalArgumentException(
        s".txt file in schema ${schemaName.normalized} not found"
      )
    }
    val ownerFilePerSchema = ownerFiles.mapValues(_.head)

    SchemasWithFilesGroupedByType(tableFiles, functionFiles, ownerFilePerSchema)
  }

}
