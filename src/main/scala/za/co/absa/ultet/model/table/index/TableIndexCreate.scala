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

import za.co.absa.ultet.dbitems.table.DBTableIndex.DBSecondaryIndex
import za.co.absa.ultet.model.table.{TableAlteration, TableIdentifier, TableName}

case class TableIndexCreate(tableIdentifier: TableIdentifier, tableIndex: DBSecondaryIndex) extends TableAlteration {

  override def sqlExpression: String = {
    val unique = if(tableIndex.unique) " UNIQUE" else ""

    val columns = tableIndex.columns.map{ indexColumn =>
      val ascDesc = if (indexColumn.ascendingOrder) "ASC" else "DESC"
      val nullsFirstLast = if (indexColumn.nullsFirst) " NULLS FIRST" else " NULLS LAST"
      s"${indexColumn.expression} $ascDesc$nullsFirstLast"
    }.mkString(", ")

    val nullsDistinct = if (tableIndex.nullsDistinct) "NULLS NOT DISTINCT" else "NULLS DISTINCT"

      s"""CREATE$unique INDEX CONCURRENTLY ${tableIndex.indexName} ON ${tableIdentifier.fullName} ($columns) $nullsDistinct;"""
  }

  override def orderInTransaction: Int = 270
}
