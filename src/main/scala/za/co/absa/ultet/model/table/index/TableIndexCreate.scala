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
package za.co.absa.ultet.model.table.index

import za.co.absa.ultet.dbitems.DBTableMember.DBTableIndex
import za.co.absa.ultet.model.SchemaName
import za.co.absa.ultet.model.table.{TableAlteration, TableName}

case class TableIndexCreate(schemaName: SchemaName, tableIndex: DBTableIndex) extends TableAlteration {

  override def tableName: TableName = TableName(tableIndex.tableName)

  override def sqlExpression: String = {
    val unique = if(tableIndex.unique) " UNIQUE" else ""
    val ascDesc = if (tableIndex.ascendingOrder) "ASC" else "DESC"
    val nullsFirstLast = tableIndex.nullsFirstOverride.map(value => if(value) " NULLS FIRST" else " NULLS LAST").getOrElse("")

    val columns = tableIndex.indexBy.map { colName =>
      s"$colName $ascDesc$nullsFirstLast"
    }.mkString(", ")

    val nullsDistinct = if (tableIndex.nullsDistinct) "NULLS NOT DISTINCT" else "NULLS DISTINCT"

      s"""CREATE$unique INDEX ${tableIndex.indexName} ON ${schemaName.value}.${tableName.value} ($columns) $nullsDistinct;"""
  }

  override def orderInTransaction: Int = 270
}
