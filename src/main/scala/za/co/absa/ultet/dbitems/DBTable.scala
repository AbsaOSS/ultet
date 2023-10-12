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

import za.co.absa.ultet.dbitems.DBTableMember._
import za.co.absa.ultet.model.table.index.{TableIndexCreate, TableIndexDrop}
import za.co.absa.ultet.model.table.alterations.{TableColumnNotNullDrop, TableColumnDefaultSet, TablePrimaryKeyAdd, TablePrimaryKeyDrop, TableColumnDefaultDrop}
import za.co.absa.ultet.model.table.column.{TableColumnAdd, TableColumnDrop}
import za.co.absa.ultet.model.table.{TableAlteration, TableCreation, TableEntry, TableName}
import za.co.absa.ultet.model.{ColumnName, DatabaseName, SchemaName, UserName}
import DBTable.ColumnsDifferenceResolver

//TODO checks on validity of entries
case class DBTable(
  tableName: TableName,
  schemaName: SchemaName,
  description: Option[String],
  primaryDBName: DatabaseName,
  owner: UserName,
  columns: Seq[DBTableColumn] = Seq.empty,
  primaryKey: Option[DBTablePrimaryKey] = None,
  indexes: Seq[DBTableIndex] = Seq.empty
) {
  def addColumn(column: DBTableColumn): DBTable = {
    this.copy(columns = columns ++ Seq(column))
  }

  def addIndex(index: DBTableIndex): DBTable = {
    this.copy(indexes = indexes ++ Seq(index))
  }

  def definePrimaryKey(pk: DBTablePrimaryKey): DBTable = {
    this.copy(primaryKey = Some(pk))
  }

  def -(other: Option[DBTable]): Seq[TableEntry] = {
    other match {
      case None => Seq(TableCreation(schemaName, tableName))
      case Some(definedOther) => this - definedOther
    }
  }

  def -(other: DBTable): Seq[TableEntry] = {
    assert(tableName == other.tableName, s"Table names must match to diff tables, but $tableName != ${other.tableName}")

    val removeIndices = this.indexes.filterNot(other.indexes.contains)
    val alterationsToRemoveIndices = removeIndices.map(idx => TableIndexDrop(tableName, idx.tableName))

    val addIndices = other.indexes.filterNot(this.indexes.contains)
    val alterationsToAddIndices = addIndices.map(idx => TableIndexCreate(idx))

    val pkEntries: Seq[TableAlteration] = (this.primaryKey, other.primaryKey) match {
      case (x, y) if x == y => Seq.empty
      case (Some(existingPk), Some(newPk)) => Seq(
        TablePrimaryKeyDrop(tableName, existingPk),
        TablePrimaryKeyAdd(tableName, newPk)
      )
      case (None, Some(newPk)) => Seq(TablePrimaryKeyAdd(tableName, newPk))
      case (Some(existingPk), None) => Seq(TablePrimaryKeyDrop(tableName, existingPk))
    }

    // todo alter description?

    val diffResolver = ColumnsDifferenceResolver(tableName)(columns, other.columns)

    diffResolver.alterationsForCommonColumns ++
      alterationsToRemoveIndices ++
      diffResolver.alterationsForColumnRemovals ++
      pkEntries ++
      diffResolver.alterationsForColumnAdditions ++
      alterationsToAddIndices
  }
}

object DBTable {
  case class ColumnsDifferenceResolver(tableName: TableName)(thisColumns: Seq[DBTableColumn], otherColumns: Seq[DBTableColumn]) {
    val thisColumnNames = thisColumns.map(_.columnName)
    val otherColumnNames = otherColumns.map(_.columnName)

    val columnNamesToRemove: Set[ColumnName] = {
      thisColumnNames.filterNot(otherColumnNames.contains).toSet // remove those that are not found in other.columns
    }
    val columnNamesToAdd: Seq[ColumnName] = otherColumnNames.filterNot(thisColumnNames.contains) // add those that are not found in this.columns

    val commonColumnNames = thisColumnNames.toSet ++ otherColumnNames.toSet -- columnNamesToRemove -- columnNamesToAdd

    def columnsToAdd: Seq[DBTableColumn] = otherColumns.filter(col => columnNamesToAdd.contains(col.columnName))

    def columnsToRemove: Set[DBTableColumn] = thisColumns.filter(col => columnNamesToRemove.contains(col.columnName)).toSet

    def commonColumns: Set[(DBTableColumn, DBTableColumn)] = {
      commonColumnNames.map { commonName =>
        val tCol = thisColumns.find(_.columnName == commonName).getOrElse(throw new IllegalStateException(s"could not find column $commonName in table ${tableName.value}"))
        val oCol = otherColumns.find(_.columnName == commonName).getOrElse(throw new IllegalStateException(s"could not find column $commonName in table ${tableName.value}"))

        (tCol, oCol)
      }
    }

    def alterationsForColumnAdditions: Seq[TableAlteration] = {
      columnsToAdd.map(col => TableColumnAdd(tableName, col))
    }

    def alterationsForColumnRemovals: Seq[TableAlteration] = {
      columnsToRemove.map(col => TableColumnDrop(tableName, col.columnName)).toSeq
    }

    def alterationsForCommonColumns: Seq[TableAlteration] = {
      commonColumns.flatMap { case (thisCol, thatCol) =>
        generateAlterForDataTypeChange(thisCol, thatCol) ++
          generateAlterForNotNullChange(thisCol, thatCol) ++
          generateAlterForDescriptionChange(thisCol, thatCol) ++
          generateAlterForDefaultChange(thisCol, thatCol)
      }.toSeq
    }

    def generateAlterForDataTypeChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      // todo for very specific datatype changes only? from/to String, numerics?
      throw new IllegalStateException(s"Cannot change datatype of ${thisCol.columnName.value} from ${thisCol.dataType} to ${otherCol.dataType} ")
    }

    def generateAlterForNotNullChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.notNull, otherCol.notNull) match {
        case (t, o) if t == o => Seq.empty // no change
        case (true, false) => Seq(TableColumnNotNullDrop(tableName, otherCol.columnName))
        case (false, true) => throw new IllegalStateException(s"Cannot change [null] to [not null] for ${thisCol.columnName.value} for table ${tableName.value} ")
      }
    }

    def generateAlterForDescriptionChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      Seq.empty // todo change comment
    }

    def generateAlterForDefaultChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.default, otherCol.default) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(t), None) => Seq(TableColumnDefaultDrop(tableName, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnDefaultSet(tableName, otherCol.columnName, o)) // both add/set
      }
    }

  }
}
