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

package za.co.absa.ultet.parsers.helpers

import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.dbitems.table.DBTableColumn
import za.co.absa.ultet.dbitems.table.DBTableIndex.{DBPrimaryKey, DBSecondaryIndex, IndexColumn}
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName}
import za.co.absa.ultet.model.table.{ColumnName, IndexName, TableIdentifier, TableName}

case class DBTableFromYaml(
  table: String,
  description: Option[String],
  primaryDBName: String,
  owner: String,
  columns: Seq[Map[String, String]] = Seq.empty,
  primaryKey: Option[Map[String, String]] = None,
  indexes: Seq[Map[String, String]] = Seq.empty
) {
  private def prepareColumns: Seq[DBTableColumn] = {
    columns.map(
      currCol => {
        DBTableColumn(
          ColumnName(currCol("columnName")),
          currCol("dataType"),
          currCol("notNull").toBoolean,
          currCol.get("description"),
          currCol.get("default"),
        )
      }
    )
  }

  private def prepareIndexes(tableIdentifier: TableIdentifier): Seq[DBSecondaryIndex] = {
    indexes.map(
      currIndex => {
        DBSecondaryIndex(
          tableIdentifier = tableIdentifier,
          indexName = IndexName(currIndex("indexName")),
          columns = currIndex("indexBy")
            .replaceAll("""^\[|\]$""", "")
            .split(",")
            .map(col => IndexColumn(col.trim))
            .toList,
          unique = currIndex.getOrElse("unique", "false").toBoolean,
          // todo better all this
          //currIndex.getOrElse("ascendingOrder", "true").toBoolean,
          //currIndex.get("nullsFirstOverride").map(_.toBoolean),
          nullsDistinct = currIndex.getOrElse("nullsDistinct", "true").toBoolean,
          description = None,
          constraint = None
        )
      }
    )
  }

  private def preparePrimaryKey(tableIdentifier: TableIdentifier): Option[DBPrimaryKey] = {
    if (primaryKey.isDefined) {
      val cols = primaryKey.get("columns")
      val pkName = primaryKey.get("name")
      val indexColumns = cols.replaceAll("""^\[|\]$""", "")
        .split(",")
        .map(currColName => IndexColumn(currColName.trim))
        .toList

      val preparedPk = DBPrimaryKey(
        tableIdentifier,
        IndexName(pkName),
        indexColumns,
      )
      Some(preparedPk)
    } else {
      None
    }
  }

  def convertToDBTable(schemaName: SchemaName): DBTable = {
    val semiPreparedTable = DBTable(
      schemaName,
      TableName(table),
      DatabaseName(primaryDBName),
      UserName(owner),
      description
    )

    val withColumns = prepareColumns.foldLeft(semiPreparedTable) { case (acc, preparedColumn) =>
      acc.addColumn(preparedColumn)
    }
    val withColumnsAndIndexes = prepareIndexes(semiPreparedTable.tableIdentifier).foldLeft(withColumns) { case (acc, preparedIndex) =>
      acc.addIndex(preparedIndex)
    }
    preparePrimaryKey(semiPreparedTable.tableIdentifier) match {
      case Some(pk) => withColumnsAndIndexes.definePrimaryKey(pk)
      case _ => withColumnsAndIndexes
    }
  }
}
