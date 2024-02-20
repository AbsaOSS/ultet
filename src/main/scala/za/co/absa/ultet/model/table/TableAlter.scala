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

import za.co.absa.ultet.model.DBItem
import za.co.absa.ultet.model.table.TableDef.ColumnsDifferenceResolver
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.sql.entries.table.{TableAlteration, TableDescriptionSet}
import za.co.absa.ultet.sql.entries.table.index.{TableIndexCreation, TableIndexDrop, TablePrimaryKeyAdd, TablePrimaryKeyDrop}

case class TableAlter(newTable: TableDef, origTable: TableDef) extends DBItem {

  assert(newTable.schemaName == origTable.schemaName, s"Schema names must match to diff tables, but ${newTable.schemaName} != ${origTable.schemaName}")
  assert(newTable.tableName == origTable.tableName, s"Table names must match to diff tables, but ${newTable.tableName} != ${origTable.tableName}")

  override def sqlEntries: Seq[SQLEntry] = {
    val removeIndices = newTable.indexes.diff(origTable.indexes)
    val alterationsToRemoveIndices = removeIndices.map(idx => TableIndexDrop(origTable.tableIdentifier, idx.indexName))

    val addIndices = origTable.indexes.diff(origTable.indexes)
    val alterationsToAddIndices = addIndices.map(idx => TableIndexCreation(newTable.tableIdentifier, idx))

    val description = if (origTable.description != newTable.description) {
      Seq(TableDescriptionSet(newTable.tableIdentifier, newTable.description))
    } else {
      Seq.empty
    }

    val pkEntries: Seq[TableAlteration] = (origTable.primaryKey, origTable.primaryKey) match {
      case (x, y) if x == y => Seq.empty
      case (Some(existingPk), Some(newPk)) => Seq(
        TablePrimaryKeyDrop(origTable.tableIdentifier, existingPk),
        TablePrimaryKeyAdd(newTable.tableIdentifier, newPk)
      )
      case (None, Some(newPk)) => Seq(TablePrimaryKeyAdd(newTable.tableIdentifier, newPk))
      case (Some(existingPk), None) => Seq(TablePrimaryKeyDrop(origTable.tableIdentifier, existingPk))
    }

    //TODO #38 Table comments need escaping

    val diffResolver = ColumnsDifferenceResolver(newTable.tableIdentifier)(origTable.columns, origTable.columns)

    diffResolver.alterationsForColumnAdditions ++
      diffResolver.alterationsForCommonColumns ++
      alterationsToRemoveIndices ++
      diffResolver.alterationsForColumnRemovals ++
      pkEntries ++
      alterationsToAddIndices ++
      description
  }
}
