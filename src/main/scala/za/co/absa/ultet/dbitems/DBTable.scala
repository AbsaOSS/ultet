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

import za.co.absa.ultet.model.table.{ColumnName, TableAlteration, TableCreation, TableEntry, TableName}
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName}
import za.co.absa.ultet.model.table.index.{TableIndexCreate, TableIndexDrop}
import za.co.absa.ultet.model.table.alterations.{TableColumnCommentDrop, TableColumnCommentSet, TableColumnDefaultDrop, TableColumnDefaultSet, TableColumnNotNullDrop, TablePrimaryKeyAdd, TablePrimaryKeyDrop}
import DBTable.ColumnsDifferenceResolver
import za.co.absa.ultet.dbitems.table.DBTableIndex.{DBPrimaryKey, DBSecondaryIndex}
import za.co.absa.ultet.dbitems.table.DBTableColumn
import za.co.absa.ultet.model.table.column.{TableColumnAdd, TableColumnDrop}

import java.sql.Connection

//TODO checks on validity of entries
case class DBTable(
                   tableName: TableName,
                   schemaName: SchemaName,
                   primaryDBName: DatabaseName,
                   owner: UserName,
                   description: Option[String] = None,
                   columns: List[DBTableColumn] = List.empty,
                   primaryKey: Option[DBPrimaryKey] = None,
                   indexes: Set[DBSecondaryIndex] = Set.empty
                   ) {
  def addColumn(column: DBTableColumn): DBTable = {
    this.copy(columns = columns ++ Seq(column))
  }

  def addIndex(index: DBSecondaryIndex): DBTable = {
    this.copy(indexes = indexes ++ Seq(index))
  }

  def definePrimaryKey(pk: DBPrimaryKey): DBTable = {
    this.copy(primaryKey = Some(pk))
  }

  def -(other: Option[DBTable]): Seq[TableEntry] = {
    other match {
      case None => {
        val pkCreateAlteration: Option[TableAlteration] = primaryKey.map(definedPk => TablePrimaryKeyAdd(schemaName, tableName, definedPk))
        val indicesCreateAlterations = indexes.map(idx => TableIndexCreate(schemaName, idx))

        Seq(TableCreation(schemaName, tableName, columns)) ++
          pkCreateAlteration.toSeq ++
          indicesCreateAlterations
      }

      case Some(definedOther) => this - definedOther
    }
  }

  def -(other: DBTable): Seq[TableEntry] = {
    assert(tableName == other.tableName, s"Table names must match to diff tables, but $tableName != ${other.tableName}")

    val removeIndices = this.indexes.diff(other.indexes)
    val alterationsToRemoveIndices = removeIndices.map(idx => TableIndexDrop(schemaName, tableName, idx.tableName))

    val addIndices = other.indexes.diff(this.indexes)
    val alterationsToAddIndices = addIndices.map(idx => TableIndexCreate(schemaName, idx))

    val pkEntries: Seq[TableAlteration] = (this.primaryKey, other.primaryKey) match {
      case (x, y) if x == y => Seq.empty
      case (Some(existingPk), Some(newPk)) => Seq(
        TablePrimaryKeyDrop(schemaName, tableName, existingPk),
        TablePrimaryKeyAdd(schemaName, tableName, newPk)
      )
      case (None, Some(newPk)) => Seq(TablePrimaryKeyAdd(schemaName, tableName, newPk))
      case (Some(existingPk), None) => Seq(TablePrimaryKeyDrop(schemaName, tableName, existingPk))
    }

    // todo alter description?

    val diffResolver = ColumnsDifferenceResolver(schemaName, tableName)(columns, other.columns)

    diffResolver.alterationsForColumnAdditions ++
    diffResolver.alterationsForCommonColumns ++
      alterationsToRemoveIndices ++
      diffResolver.alterationsForColumnRemovals
    // pkEntries ++ todo #94
    // alterationsToAddIndices ++ todo #94

  }
}

object DBTable {
  case class ColumnsDifferenceResolver(schemaName: SchemaName, tableName: TableName)(thisColumns: Seq[DBTableColumn], otherColumns: Seq[DBTableColumn]) {
    private[dbitems] val thisColumnNames = thisColumns.map(_.columnName)
    private[dbitems] val otherColumnNames = otherColumns.map(_.columnName)

    private[dbitems] val columnNamesToRemove: Set[ColumnName] = {
      thisColumnNames.filterNot(otherColumnNames.contains).toSet // remove those that are not found in other.columns
    }
    private[dbitems] val columnNamesToAdd: Seq[ColumnName] = otherColumnNames.filterNot(thisColumnNames.contains) // add those that are not found in this.columns

    private[dbitems] val commonColumnNames = thisColumnNames.toSet ++ otherColumnNames.toSet -- columnNamesToRemove -- columnNamesToAdd

    private[dbitems] def columnsToAdd: Seq[DBTableColumn] = otherColumns.filter(col => columnNamesToAdd.contains(col.columnName))

    private[dbitems] def columnsToRemove: Set[DBTableColumn] = thisColumns.filter(col => columnNamesToRemove.contains(col.columnName)).toSet

    private[dbitems] def commonColumns: Set[(DBTableColumn, DBTableColumn)] = {
      commonColumnNames.map { commonName =>
        val tCol = thisColumns.find(_.columnName == commonName).getOrElse(throw new IllegalStateException(s"could not find column $commonName in table ${schemaName.value}.${tableName.value}"))
        val oCol = otherColumns.find(_.columnName == commonName).getOrElse(throw new IllegalStateException(s"could not find column $commonName in table ${schemaName.value}.${tableName.value}"))

        (tCol, oCol)
      }
    }

    def alterationsForColumnAdditions: Seq[TableAlteration] = {
      columnsToAdd.map(col => TableColumnAdd(schemaName, tableName, col))
    }

    def alterationsForColumnRemovals: Seq[TableAlteration] = {
      columnsToRemove.map(col => TableColumnDrop(schemaName, tableName, col.columnName)).toSeq
    }

    def alterationsForCommonColumns: Seq[TableAlteration] = {
      commonColumns.flatMap { case (thisCol, thatCol) =>
        generateAlterForDataTypeChange(thisCol, thatCol) ++
          generateAlterForNotNullChange(thisCol, thatCol) ++
          generateAlterForDescriptionChange(thisCol, thatCol) ++
          generateAlterForDefaultChange(thisCol, thatCol)
      }.toSeq
    }

    private def generateAlterForDataTypeChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      // todo for very specific datatype changes only? from/to String, numerics?
      throw new IllegalStateException(s"Cannot change datatype of ${thisCol.columnName.value} from ${thisCol.dataType} to ${otherCol.dataType} ")
    }

    private def generateAlterForNotNullChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.notNull, otherCol.notNull) match {
        case (t, o) if t == o => Seq.empty // no change
        case (true, false) => Seq(TableColumnNotNullDrop(schemaName, tableName, otherCol.columnName))
        case (false, true) => throw new IllegalStateException(s"Cannot change [null] to [not null] for ${thisCol.columnName.value} for table ${schemaName.value}.${tableName.value} ")
      }
    }

    private def generateAlterForDescriptionChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.description, otherCol.description) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(t), None) => Seq(TableColumnCommentDrop(schemaName, tableName, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnCommentSet(schemaName, tableName, otherCol.columnName, o)) // both add/set
      }
    }

    private def generateAlterForDefaultChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.default, otherCol.default) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(t), None) => Seq(TableColumnDefaultDrop(schemaName, tableName, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnDefaultSet(schemaName, tableName, otherCol.columnName, o)) // both add/set
      }
    }

  }
  def createFromPG(schemaName: SchemaName, tableName: TableName, databaseName: DatabaseName)
                  (implicit jdbcConnection: Option[Connection]): Option[DBTable] = {
    jdbcConnection.flatMap { dbConnection =>
      val extractor = new ExtractorOfDBTable(schemaName, tableName)(dbConnection)
      extractor.owner.map { owner =>
        DBTable(
          tableName,
          schemaName,
          databaseName,
          UserName(owner),
          extractor.description,
          extractor.columns,
          extractor.primaryKey,
          extractor.indexes
        )
      }
    }
  }
}
