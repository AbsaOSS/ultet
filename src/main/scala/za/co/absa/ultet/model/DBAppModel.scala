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

package za.co.absa.ultet.model

import za.co.absa.balta.classes.DBConnection
import za.co.absa.ultet.implicits.SetImplicits.DBItemSetEnhancement
import za.co.absa.ultet.model.function.{FunctionFromPG, FunctionHeader}
import za.co.absa.ultet.model.schema.SchemaDef
import za.co.absa.ultet.model.table.TableDef
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.complex.{DatabaseDefs, SqlEntriesPerTransaction}
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.util.{FileReader, TaskConfig}
import za.co.absa.ultet.util.FileReader.SchemaFiles
import za.co.absa.ultet.util.SourceFileType.{FunctionSrc, SchemaOwner, TableSrc}
import za.co.absa.ultet.util.extractors.DBTableFromPG
import za.co.absa.ultet.util.parsers.{PgFunctionFileParser, PgTableFileParser}

import java.net.URI

case class DBAppModel(databases: DatabaseDefs) {

  def +(other: Option[DBAppModel]): DBAppModel = {
    other.fold(this)(this + _)
  }

  def +(other: DBAppModel): DBAppModel = {
    val newDatabases = other.databases.foldLeft(databases) {
      case (acc, (dbName, dbDef)) =>
        val newDbDef = dbDef + acc.get(dbName)
        acc + (dbName -> newDbDef)
    }
    DBAppModel(newDatabases)
  }

  def addFunctionSource(schemaName: SchemaName, functionSource: String): DBAppModel = {
    val functions = PgFunctionFileParser.parseSource(functionSource)
    val newDBAppModel = DBAppModel.fromDBFunctions(functions.toSeq) //we need to use Seq instead of Set because Seq allows covariance while Set only invariance
    //TODO #31 Add warnings to the system - check if the function belong to the provided schema

    this + newDBAppModel
  }

  def addTableSource(schemaName: SchemaName, tableSource: String): DBAppModel = {
    val tables = PgTableFileParser(schemaName).parseSource(tableSource)
    val newDBAppModel = DBAppModel.fromTableDefs(tables)
    this + newDBAppModel
  }

  def addDatabasesAnalysis()(implicit taskConfig: TaskConfig): DBAppModel = {
    val newDatabases = databases.foldLeft(this.databases) {
      case (acc, (dbName, dbDef)) =>
        val newDbDef = analyzeDatabase(dbDef)
        acc + (dbName -> (dbDef + newDbDef))
    }
    copy(databases = newDatabases)
  }

  def toDBItems: Map[DatabaseName, Set[DBItem]] = {
    databases.map{case (dbName, dbDef) => dbName -> dbDef.toDBItems}
  }

  def createSQLEntries(): Map[DatabaseName, SqlEntriesPerTransaction] = {
    databases.map{case (dbName, dbDef) =>
      val sqlEntries = dbDef.toDBItems.toSortedGroupedSqlEntries
      dbName -> sqlEntries
    }
  }


  private def analyzeDatabase(dbDef: DatabaseDef)(implicit taskConfig: TaskConfig): DatabaseDef = {
    implicit val dbConnection: DBConnection = taskConfig.dbConnections(dbDef.databaseName).dbConnection
    val tableExtractor = DBTableFromPG(dbDef.databaseName)
    val newSchemas = dbDef.schemas.foldLeft(dbDef.schemas) {
      case (acc, (schemaName, schemaDef)) =>
        val functionsInDatabase: Seq[FunctionHeader] =
          FunctionFromPG.fetchAllOfSchema(dbDef.databaseName, schemaName)(dbConnection.connection) //TODO  #33 Get Postgres functions based on list of names
        val tablesInDatabase = schemaDef
          .tablesFromSource
          .values.flatMap(table => tableExtractor.extract(schemaName, table.tableName))
          .map(table => table.tableName -> table)
          .toMap
        val newSchemaDef = SchemaDef(schemaName, ownerNameX = None, functionsInDatabase.toSet, Map.empty, tablesInDatabase) //TODO #30 Schema owner support
        acc + (schemaName -> newSchemaDef)
    }
    DatabaseDef(dbDef.databaseName, newSchemas, createDatabase = false)
    // TODO #32 Add database creation support - in case the database cannot be contacted
  }
}

object DBAppModel {

  def loadFromSources(sources: SchemaFiles): DBAppModel = {
    sources.foldLeft(emptyDBAppModel) {
      case (acc, (schemaName, files)) =>
        val newDBAppModel = loadSchemaFiles(schemaName, files)
        acc + newDBAppModel
    }
  }

  def fromDBFunctions[T <: FunctionHeader](functions: Seq[T]): DBAppModel = {
    val databases = functions.groupBy(_.database)

    val databaseDefs = databases.map {
      case (dbName, dbFunctions) =>
        val schemas = dbFunctions.groupBy(_.schema)
        val schemaDefs = schemas.map {
          case (schemaName, schemaFunctions) =>
            val schemaDef = schema.SchemaDef(schemaName, ownerNameX = None,schemaFunctions.toSet, Map.empty, Map.empty) //TODO #30 Schema owner support
            schemaName -> schemaDef
        }
        dbName -> DatabaseDef(dbName, schemaDefs, createDatabase = false)
    }

    DBAppModel(databaseDefs)
  }

  private def fromTableDefs(tables: Set[TableDef]): DBAppModel = {
    val tablesByDB = tables.groupBy(_.primaryDBName)

    val databaseDefs = tablesByDB.map {
      case (dbName, dbTables) =>
        val schemaDef = dbTables.groupBy(_.schemaName).map {
          case (schemaName, schemaTables) =>
            val schemaTablesMap = schemaTables.map(table => table.tableName -> table).toMap
            val schemaDef = SchemaDef(schemaName, ownerNameX = None , Set.empty, schemaTablesMap, Map.empty) //TODO #30 Schema owner support
            schemaName -> schemaDef
        }
        dbName -> DatabaseDef(dbName, schemaDef, createDatabase = false)
    }

    DBAppModel(databaseDefs)
  }

  private def loadSchemaFiles(schemaName: SchemaName, files: Set[URI]): DBAppModel = {
    files.foldLeft(emptyDBAppModel) {
      case (acc, file) =>
        FileReader.fileType(file) match {
          case TableSrc    => acc.addTableSource(schemaName, FileReader.readFileAsString(file))
          case FunctionSrc => acc.addFunctionSource(schemaName, FileReader.readFileAsString(file))
          case SchemaOwner => acc // TODO #30 Schema owner support
          case _           => acc
        }
    }
  }

  private val emptyDBAppModel = DBAppModel(Map.empty[DatabaseName, DatabaseDef])
}
