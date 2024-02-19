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

import za.co.absa.ultet.model.table.TableMember
import za.co.absa.ultet.model.table.index.TableIndex.IndexColumn
import za.co.absa.ultet.types.table.{IndexName, TableIdentifier}

trait TableIndex extends TableMember {
  def  tableIdentifier: TableIdentifier
  def indexName: IndexName
  def columns: Seq[IndexColumn]
  def description: Option[String]
}

object TableIndex {
  case class PrimaryKey(
                            tableIdentifier: TableIdentifier,
                            indexName: IndexName,
                            columns: Seq[IndexColumn],
                            description: Option[String] = None
                          ) extends TableIndex {
    private[ultet] def joinColumns(other: TableIndex): PrimaryKey = copy(columns = columns ++ other.columns)
  }

  case class SecondaryIndex(
                                tableIdentifier: TableIdentifier,
                                indexName: IndexName,
                                columns: Seq[IndexColumn],
                                description: Option[String] = None,
                                unique: Boolean = false,
                                nullsDistinct: Boolean = true,
                                constraint: Option[String] = None
                              ) extends TableIndex {
    private[ultet] def joinColumns(other: TableIndex): SecondaryIndex = copy(columns = columns ++ other.columns)
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
