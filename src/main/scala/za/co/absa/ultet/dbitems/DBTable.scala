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

import za.co.absa.ultet.model.table.{ColumnName, TableAlteration, TableIdentifier, TableName}
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName}
import za.co.absa.ultet.model.table.alterations.{TableColumnCommentDrop, TableColumnCommentSet, TableColumnDefaultDrop, TableColumnDefaultSet, TableColumnNotNullDrop}
import za.co.absa.balta.classes.DBConnection
import za.co.absa.ultet.dbitems.extractors.DBTableFromPG
import za.co.absa.ultet.dbitems.table.DBTableIndex.{DBPrimaryKey, DBSecondaryIndex}
import za.co.absa.ultet.dbitems.table.DBTableColumn
import za.co.absa.ultet.implicits.OptionImplicits.OptionEnhancements
import za.co.absa.ultet.model.table.column.{TableColumnAdd, TableColumnDrop}

import java.sql.Connection

//TODO  #31 Add warnings to the system, checks on validity of entries
case class DBTable(
                   tableIdentifier: TableIdentifier,
                   primaryDBName: DatabaseName,
                   owner: UserName,
                   description: Option[String],
                   columns: Seq[DBTableColumn],
                   primaryKey: Option[DBPrimaryKey],
                   indexes: Set[DBSecondaryIndex]
                   ) {

  def schemaName: SchemaName = tableIdentifier.schemaName

  def tableName: TableName = tableIdentifier.tableName


  def addColumn(column: DBTableColumn): DBTable = {
    this.copy(columns = columns ++ Seq(column))
  }

  def addIndex(index: DBSecondaryIndex): DBTable = {
    this.copy(indexes = indexes ++ Seq(index))
  }

  def definePrimaryKey(pk: DBPrimaryKey): DBTable = {
    this.copy(primaryKey = Some(pk))
  }

  def -(other: Option[DBTable]): DBItem = {
    other.map(this - _).getOrElse(DBTableInsert(this))
  }

  def -(other: DBTable): DBItem = {
    DBTableAlter(this, other)
  }
}

object DBTable {

  def apply(schemaName: SchemaName,
            tableName: TableName,
            primaryDBName: DatabaseName,
            owner: UserName,
            description: Option[String] = None,
            columns: Seq[DBTableColumn] = Seq.empty,
            primaryKey: Option[DBPrimaryKey] = None,
            indexes: Set[DBSecondaryIndex] = Set.empty
           ): DBTable = {
    new DBTable(TableIdentifier(schemaName, tableName), primaryDBName, owner, description, columns, primaryKey, indexes)
  }

  case class ColumnsDifferenceResolver(tableIdentifier: TableIdentifier)(thisColumns: Seq[DBTableColumn], otherColumns: Seq[DBTableColumn]) {
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
        val tCol = thisColumns
          .find(_.columnName == commonName)
          .getOrThrow(new IllegalStateException(s"could not find column $commonName in table ${tableIdentifier.fullName}"))
        val oCol = otherColumns
          .find(_.columnName == commonName)
          .getOrThrow(new IllegalStateException(s"could not find column $commonName in table ${tableIdentifier.fullName}"))

        (tCol, oCol)
      }
    }

    def alterationsForColumnAdditions: Seq[TableAlteration] = {
      columnsToAdd.map(col => TableColumnAdd(tableIdentifier, col))
    }

    def alterationsForColumnRemovals: Seq[TableAlteration] = {
      columnsToRemove.map(col => TableColumnDrop(tableIdentifier, col.columnName)).toSeq
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
      // TODO #37 Allow certain data type changes
      throw new IllegalStateException(s"Cannot change datatype of ${thisCol.columnName.value} from ${thisCol.dataType} to ${otherCol.dataType} ")
    }

    private def generateAlterForNotNullChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.notNull, otherCol.notNull) match {
        case (t, o) if t == o => Seq.empty // no change
        case (true, false) => Seq(TableColumnNotNullDrop(tableIdentifier, otherCol.columnName))
        case (false, true) => throw new IllegalStateException(s"Cannot change [null] to [not null] for ${thisCol.columnName.value} for table ${tableIdentifier.fullName} ")
      }
    }

    private def generateAlterForDescriptionChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.description, otherCol.description) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(_), None) => Seq(TableColumnCommentDrop(tableIdentifier, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnCommentSet(tableIdentifier, otherCol.columnName, o)) // both add/set
      }
    }

    private def generateAlterForDefaultChange(thisCol: DBTableColumn, otherCol: DBTableColumn): Seq[TableAlteration] = {
      (thisCol.default, otherCol.default) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(t), None) => Seq(TableColumnDefaultDrop(tableIdentifier, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnDefaultSet(tableIdentifier, otherCol.columnName, o)) // both add/set
      }
    }

  }
  def createFromPG(schemaName: SchemaName, tableName: TableName, databaseName: DatabaseName)
                  (implicit jdbcConnection: Option[Connection]): Option[DBTable] = {
    jdbcConnection.flatMap { dbConnection =>
      val extractor = DBTableFromPG(databaseName)(new DBConnection(dbConnection))
      extractor.extract(schemaName, tableName)
    }
  }
}
