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

package za.co.absa.ultet.model.table

import za.co.absa.ultet.dbitems.table.DBTableColumn
import za.co.absa.ultet.model.SchemaName

case class TableCreation(
  schemaName: SchemaName,
  tableName: TableName,
  columns: Seq[DBTableColumn],
) extends TableEntry {

  override def sqlExpression: String = {
    val columnLines = columns.map { col =>
      val notNull = if(col.notNull)" NOT NULL" else ""
      val default = col.default.map(cDef =>s" DEFAULT $cDef").getOrElse("")
      s"${col.columnName.value} ${col.dataType}$notNull$default"
    }

    s"""CREATE TABLE ${schemaName.value}.${tableName.value}(
       |${columnLines.mkString(",\n")}
       |);""".stripMargin
  }

  override def orderInTransaction: Int = 200
}
