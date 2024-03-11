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
import za.co.absa.ultet.types.complex.{DatabaseDefs, SchemaOwners, SqlEntriesPerTransaction}
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.user.UserName
import za.co.absa.ultet.util.{FileReader, TaskConfig}
import za.co.absa.ultet.util.FileReader.SchemaFiles
import za.co.absa.ultet.util.SourceFileType.{FunctionSrc, SchemaOwner, TableSrc}
import za.co.absa.ultet.util.extractors.DBTableFromPG
import za.co.absa.ultet.util.parsers.{PgFunctionFileParser, PgTableFileParser}

import java.net.URI

case class DBAppModel private[model](databases: DatabaseDefs, schemaOwners: SchemaOwners) {

  def +(other: Option[DBAppModel]): DBAppModel = {
    other.fold(this)(this + _)
  }

  def +(other: DBAppModel): DBAppModel = {

    def assignNewSchemaOwners(toModel: DBAppModel, schemas: Set[SchemaName], schemaOwnersToApply: SchemaOwners): DBAppModel = {
      schemas.foldLeft(toModel) {
        case (acc, schemaName) =>
          acc.addSchemaOwner(schemaName, schemaOwnersToApply(schemaName))
      }
    }
    // merge owners
    val thisSchemasWithOwners = schemaOwners.keys.toSet
    val otherSchemasWithOwners = other.schemaOwners.keys.toSet

    val thisWithNewOwners = assignNewSchemaOwners(this, otherSchemasWithOwners.diff(thisSchemasWithOwners), other.schemaOwners)
    val otherWithNewOwners = assignNewSchemaOwners(other, thisSchemasWithOwners.diff(otherSchemasWithOwners), schemaOwners)

    val mergedSchemaOwners = other.schemaOwners ++ schemaOwners

    // merge databases
    val newDatabases = otherWithNewOwners.databases.foldLeft(thisWithNewOwners.databases) {
      case (acc, (dbName, dbDef)) =>
        val newDbDef = dbDef + acc.get(dbName)
        acc + (dbName -> newDbDef)
    }

    DBAppModel(newDatabases, mergedSchemaOwners)
  }

  def addFunctionSource(schemaName: SchemaName, functionSource: String): DBAppModel = {
    val functions = PgFunctionFileParser.parseSource(functionSource)
    val newDBAppModel = DBAppModel.fromDBFunctions(
      functions.toSeq, //we need to use Seq instead of Set because Seq allows covariance while Set only invariance
      schemaOwners
    )
    //TODO #31 Add warnings to the system - check if the function belong to the provided schema

    this + newDBAppModel
  }

  def addTableSource(schemaName: SchemaName, tableSource: String): DBAppModel = {
    val tables = PgTableFileParser(schemaName).parseSource(tableSource)
    val newDBAppModel = DBAppModel.fromTableDefs(tables, schemaOwners)
    this + newDBAppModel
  }

  def addSchemaOwner(schemaName: SchemaName, ownerName: UserName): DBAppModel = {
    if (schemaOwners.contains(schemaName)) {
      //TODO #31 Add warnings to the system - warn if owner already exists and differ
      this
    } else {
      val newDatabases = databases.foldLeft(databases) {
        case (acc, (dbName, dbDef)) =>
          if (dbDef.schemas.contains(schemaName)) {
            val newDbDef = dbDef.copy(schemas = dbDef.schemas + (schemaName -> dbDef.schemas(schemaName).copy(ownerName = Some(ownerName))))
            acc + (dbName -> newDbDef)
          } else {
            acc
          }
      }
      val newSchemaOwners = schemaOwners + (schemaName -> ownerName)
      DBAppModel(newDatabases, newSchemaOwners)
    }
  }

  def addDatabasesAnalysis()(implicit taskConfig: TaskConfig): DBAppModel = {
    val newDatabases = databases.foldLeft(this.databases) {
      case (acc, (dbName, dbDef)) =>
        val newDbDef = analyzeDatabase(dbDef, schemaOwners)
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


  private def analyzeDatabase(dbDef: DatabaseDef, schemaOwners: SchemaOwners)(implicit taskConfig: TaskConfig): DatabaseDef = {
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
        val schemaOwner = schemaOwners.get(schemaName)
        val newSchemaDef = SchemaDef(schemaName, schemaOwner, functionsInDatabase.toSet, Map.empty, tablesInDatabase)
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

  def fromDBFunctions[T <: FunctionHeader](functions: Seq[T], schemaOwners: SchemaOwners): DBAppModel = {
    val databases = functions.groupBy(_.database)

    val databaseDefs = databases.map {
      case (dbName, dbFunctions) =>
        val schemas = dbFunctions.groupBy(_.schema)
        val schemaDefs = schemas.map {
          case (schemaName, schemaFunctions) =>
            val schemaOwner = schemaOwners.get(schemaName)
            val schemaDef = schema.SchemaDef(schemaName, schemaOwner,schemaFunctions.toSet, Map.empty, Map.empty)
            schemaName -> schemaDef
        }
        dbName -> DatabaseDef(dbName, schemaDefs, createDatabase = false)
    }

    DBAppModel(databaseDefs, schemaOwners)
  }

  private def fromTableDefs(tables: Set[TableDef], schemaOwners: SchemaOwners): DBAppModel = {
    val tablesByDB = tables.groupBy(_.primaryDBName)

    val databaseDefs = tablesByDB.map {
      case (dbName, dbTables) =>
        val schemaDef = dbTables.groupBy(_.schemaName).map {
          case (schemaName, schemaTables) =>
            val schemaTablesMap = schemaTables.map(table => table.tableName -> table).toMap
            val schemaOwner = schemaOwners.get(schemaName)
            val schemaDef = SchemaDef(schemaName, schemaOwner , Set.empty, schemaTablesMap, Map.empty)
            schemaName -> schemaDef
        }
        dbName -> DatabaseDef(dbName, schemaDef, createDatabase = false)
    }

    DBAppModel(databaseDefs, schemaOwners)
  }

  private def loadSchemaFiles(schemaName: SchemaName, files: Set[URI]): DBAppModel = {
    files.foldLeft(emptyDBAppModel) {
      case (acc, file) =>
        FileReader.fileType(file) match {
          case TableSrc    => acc.addTableSource(schemaName, FileReader.readFileAsString(file))
          case FunctionSrc => acc.addFunctionSource(schemaName, FileReader.readFileAsString(file))
          case SchemaOwner => acc.addSchemaOwner(schemaName, UserName(FileReader.readFileAsString(file, trim = true)))
          case _           => acc //TODO #31 Add warnings to the system - unrecognized file
        }
    }
  }

  private val emptyDBAppModel = DBAppModel(Map.empty[DatabaseName, DatabaseDef], Map.empty[SchemaName, UserName])
}
