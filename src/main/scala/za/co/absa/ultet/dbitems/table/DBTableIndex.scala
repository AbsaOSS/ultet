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

package za.co.absa.ultet.dbitems.table

import za.co.absa.ultet.dbitems.table.DBTableIndex.IndexColumn
import za.co.absa.ultet.model.table.{IndexName, TableName}

trait DBTableIndex extends DBTableMember {
  def  tableName: TableName
  def indexName: IndexName
  def columns: Seq[IndexColumn]
  def description: Option[String]
}

object DBTableIndex {
  case class DBPrimaryKey (
                            tableName: TableName,
                            indexName: IndexName,
                            columns: Seq[IndexColumn],
                            description: Option[String] = None
                          ) extends DBTableIndex {
    private[dbitems] def joinColumns(other: DBTableIndex): DBPrimaryKey = copy(columns = columns ++ other.columns)
  }

  case class DBSecondaryIndex (
                                tableName: TableName,
                                indexName: IndexName,
                                columns: Seq[IndexColumn],
                                description: Option[String] = None,
                                unique: Boolean = false,
                                nullsDistinct: Boolean = true,
                                constraint: Option[String] = None
                              ) extends DBTableIndex {
    private[dbitems] def joinColumns(other: DBTableIndex): DBSecondaryIndex = copy(columns = columns ++ other.columns)
  }


  case class IndexColumn(
                         expression: String, // this is not a ColumnName because it can be an expression
                         ascendingOrder: Boolean = true,
                         nullsFirstDefined: Option[Boolean] = None
                        ) {
    def nullsFirst: Boolean = {
      nullsFirstDefined.getOrElse(!ascendingOrder)
    }
  }
}
