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

import za.co.absa.balta.classes.DBConnection
import za.co.absa.ultet.implicits.OptionImplicits.OptionEnhancements
import za.co.absa.ultet.model.table.column.TableColumn
import za.co.absa.ultet.model.table.index.TableIndex.{PrimaryKey, SecondaryIndex}
import za.co.absa.ultet.model.DBItem
import za.co.absa.ultet.sql.entries.table.TableAlteration
import za.co.absa.ultet.sql.entries.table.column.{TableColumnAdd, TableColumnDescriptionSet, TableColumnDefaultDrop, TableColumnDefaultSet, TableColumnDrop, TableColumnNotNullDrop}
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{ColumnName, TableIdentifier, TableName}
import za.co.absa.ultet.types.user.UserName
import za.co.absa.ultet.util.extractors.DBTableFromPG

import java.sql.Connection

//TODO  #31 Add warnings to the system, checks on validity of entries
case class TableDef(
                     tableIdentifier: TableIdentifier,
                     primaryDBName: DatabaseName,
                     owner: UserName,
                     description: Option[String],
                     columns: Seq[TableColumn],
                     primaryKey: Option[PrimaryKey],
                     indexes: Set[SecondaryIndex]
                     ) {

  def schemaName: SchemaName = tableIdentifier.schemaName

  def tableName: TableName = tableIdentifier.tableName


  def addColumn(column: TableColumn): TableDef = {
    this.copy(columns = columns ++ Seq(column))
  }

  def addIndex(index: SecondaryIndex): TableDef = {
    this.copy(indexes = indexes ++ Seq(index))
  }

  def definePrimaryKey(pk: PrimaryKey): TableDef = {
    this.copy(primaryKey = Some(pk))
  }

  def -(other: Option[TableDef]): DBItem = {
    other.map(this - _).getOrElse(TableInsert(this))
  }

  def -(other: TableDef): DBItem = {
    TableAlter(this, other)
  }
}

object TableDef {

  def apply(schemaName: SchemaName,
            tableName: TableName,
            primaryDBName: DatabaseName,
            owner: UserName,
            description: Option[String] = None,
            columns: Seq[TableColumn] = Seq.empty,
            primaryKey: Option[PrimaryKey] = None,
            indexes: Set[SecondaryIndex] = Set.empty
           ): TableDef = {
    new TableDef(TableIdentifier(schemaName, tableName), primaryDBName, owner, description, columns, primaryKey, indexes)
  }

  case class ColumnsDifferenceResolver(tableIdentifier: TableIdentifier)(thisColumns: Seq[TableColumn], otherColumns: Seq[TableColumn]) {
    private val thisColumnNames = thisColumns.map(_.columnName)
    private val otherColumnNames = otherColumns.map(_.columnName)

    private val columnNamesToRemove: Set[ColumnName] = {
      thisColumnNames.filterNot(otherColumnNames.contains).toSet // remove those that are not found in other.columns
    }
    private val columnNamesToAdd: Seq[ColumnName] = otherColumnNames.filterNot(thisColumnNames.contains) // add those that are not found in this.columns

    private val commonColumnNames = thisColumnNames.toSet ++ otherColumnNames.toSet -- columnNamesToRemove -- columnNamesToAdd

    private def columnsToAdd: Seq[TableColumn] = otherColumns.filter(col => columnNamesToAdd.contains(col.columnName))

    private def columnsToRemove: Set[TableColumn] = thisColumns.filter(col => columnNamesToRemove.contains(col.columnName)).toSet

    private def commonColumns: Set[(TableColumn, TableColumn)] = {
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

    private def generateAlterForDataTypeChange(thisCol: TableColumn, otherCol: TableColumn): Seq[TableAlteration] = {
      // TODO #37 Allow certain data type changes
      throw new IllegalStateException(s"Cannot change datatype of ${thisCol.columnName.value} from ${thisCol.dataType} to ${otherCol.dataType} ")
    }

    private def generateAlterForNotNullChange(thisCol: TableColumn, otherCol: TableColumn): Seq[TableAlteration] = {
      (thisCol.notNull, otherCol.notNull) match {
        case (t, o) if t == o => Seq.empty // no change
        case (true, false) => Seq(TableColumnNotNullDrop(tableIdentifier, otherCol.columnName))
        case (false, true) => throw new IllegalStateException(s"Cannot change [null] to [not null] for ${thisCol.columnName.value} for table ${tableIdentifier.fullName} ")
      }
    }

    private def generateAlterForDescriptionChange(thisCol: TableColumn, otherCol: TableColumn): Seq[TableAlteration] = {
      if (thisCol.description != otherCol.description) {
        Seq(TableColumnDescriptionSet(tableIdentifier, otherCol.columnName, otherCol.description)) // add/un-/set
      } else {
        Seq.empty // no change
      }
    }

    private def generateAlterForDefaultChange(thisCol: TableColumn, otherCol: TableColumn): Seq[TableAlteration] = {
      (thisCol.default, otherCol.default) match {
        case (t, o) if t == o => Seq.empty // no change
        case (Some(t), None) => Seq(TableColumnDefaultDrop(tableIdentifier, thisCol.columnName))
        case (_, Some(o)) => Seq(TableColumnDefaultSet(tableIdentifier, otherCol.columnName, o)) // both add/set
      }
    }

  }
  def createFromPG(schemaName: SchemaName, tableName: TableName, databaseName: DatabaseName)
                  (implicit jdbcConnection: Option[Connection]): Option[TableDef] = {
    jdbcConnection.flatMap { dbConnection =>
      val extractor = DBTableFromPG(databaseName)(new DBConnection(dbConnection))
      extractor.extract(schemaName, tableName)
    }
  }
}
