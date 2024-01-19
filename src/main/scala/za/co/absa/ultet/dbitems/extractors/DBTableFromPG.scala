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

package za.co.absa.ultet.dbitems.extractors

import za.co.absa.balta.classes.setter.{Params, SetterFnc}
import za.co.absa.balta.classes.{DBConnection, DBQuerySupport, QueryResultRow}
import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.dbitems.table.{DBTableColumn, DBTableIndex}
import za.co.absa.ultet.dbitems.table.DBTableIndex.{DBPrimaryKey, DBSecondaryIndex, IndexField}
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName}
import za.co.absa.ultet.model.table.{ColumnName, TableName}

import scala.io.Source

case class DBTableFromPG(databaseName: DatabaseName)(implicit connection: DBConnection) extends DBQuerySupport {
  def extract(implicit schemaName: SchemaName, tableName: TableName): Option[DBTable] = {
    implicit val schemaAndTableNameSetters: List[SetterFnc] = Params.add(schemaName.normalized).add(tableName.normalized).setters
    extractDBTable().map {case (dbTable, hasIndexes) =>

      val columns = extractColumns()
      val (secondaryIndexes, primaryKey) = if (hasIndexes) extractIndexes() else (List.empty, None)
      dbTable.copy(columns = columns, primaryKey = primaryKey, indexes = secondaryIndexes.toSet)
    }
  }


  private def extractDBTable()(implicit schemaName: SchemaName,
                               tableName: TableName,
                               schemaAndTableNameSetters: List[SetterFnc]): Option[(DBTable, Boolean)] = {
    runQuery(readSQLFileAsString("table_indexes"), schemaAndTableNameSetters){ queryResult =>
      if (queryResult.isEmpty) {
        None
      } else {
        val row = queryResult.next()
        val dbTable = DBTable(
          tableName = tableName,
          schemaName = schemaName,
          primaryDBName = databaseName,
          owner = UserName(row.getString("table_owner").get),
          description = row.getString("description")
        )
        Some(dbTable, row.getBoolean("has_indexes").get)
      }
    }
  }
  private def extractColumns()(implicit schemaAndTableName: List[SetterFnc]): List[DBTableColumn] = {
    ???
  }

  private def extractIndexes()(implicit schemaAndTableName: List[SetterFnc]): (Seq[DBSecondaryIndex], Option[DBPrimaryKey]) = {
    val indexRows = runQuery(readSQLFileAsString("table_indexes"), schemaAndTableName)(_.map(queryResultRowToIndex).toList.groupBy(_.indexName))
    indexRows.values.foldLeft((List.empty[DBSecondaryIndex], Option.empty[DBPrimaryKey])) { case ((secondaryIndexesAcc, primKeyAcc), indexRows) =>
      indexRows.head match {
        case head: DBPrimaryKey =>
          val newPrimKey = indexRows.foldLeft(head)((acc, item) => acc.joinColumns(item))
          (secondaryIndexesAcc, Some(newPrimKey))
        case head: DBSecondaryIndex =>
          val newIndex = indexRows.foldLeft(head)((acc, item) => acc.joinColumns(item))
          (newIndex +: secondaryIndexesAcc, primKeyAcc)
      }
    }
  }

  private def queryResultRowToIndex(indexRow: QueryResultRow): DBTableIndex = {
    def readColumn(): List[IndexField] = List(IndexField(
      expression = indexRow.getString("column_expression").get,
      ascendingOrder = indexRow.getBoolean("is_ascending").get,
      nullsFirstDefined = indexRow.getBoolean("nulls_first")
    ))

    if (indexRow.getBoolean("is_primary").get) {
      DBPrimaryKey(
        tableName = indexRow.getString("table_name").get,
        indexName = indexRow.getString("index_name").get,
        columns = readColumn(),
        description = indexRow.getString("description")
      )
    } else {
      DBSecondaryIndex(
        tableName = indexRow.getString("table_name").get,
        indexName = indexRow.getString("index_name").get,
        columns = readColumn(),
        description = indexRow.getString("description"),
        unique = indexRow.getBoolean("is_unique").get,
        nullsDistinct = indexRow.getBoolean("nulls_distinct").get,
        constraint = indexRow.getString("constraint_expression")
      )
    }
  }

  private def queryResultRowToColumn(columnRow: QueryResultRow): DBTableColumn = {
    DBTableColumn(
      columnName = ColumnName(columnRow.getString("column_name").get),
      dataType = queryResultRowToDataType(columnRow),
      notNull = columnRow.getBoolean("is_nullable").get,
      description = columnRow.getString("description"),
      default = columnRow.getString("default")
    )
  }

  private def queryResultRowToDataType(columnRow: QueryResultRow): String = {
    ???
  }

  private def readSQLFileAsString(sqlFilename: String, lineSeparator: String = "\n"): String = {
    val filename = s"queries/$sqlFilename.sql"
    val sourceFile = Source.fromResource(filename)
    try {
      sourceFile.getLines().mkString(lineSeparator)
    } finally {
      sourceFile.close()
    }
  }
}
