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

package za.co.absa.ultet

import com.typesafe.scalalogging.Logger
import scopt.OParser
import za.co.absa.balta.classes.DBConnection
import za.co.absa.ultet.dbitems.DBItem
import za.co.absa.ultet.implicits.MapImplicits.SqlEntriesPerTransactionEnhancement
import za.co.absa.ultet.implicits.OptionImplicits.OptionEnhancements
import za.co.absa.ultet.model.{DBAppModel, DatabaseName, SQLEntry, TransactionGroup}
import za.co.absa.ultet.util.FileReader.SchemaFiles
import za.co.absa.ultet.util.{CliParser, Config, DBProperties, FileReader, SqlEntriesPerTransaction, SqlExecutor, SqlsPerTransaction, TaskConfig}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.util.{Failure, Success, Try}

object Ultet {
  private val logger = Logger(getClass.getName)

  def main(args: Array[String]): Unit = {

    implicit val (appConfig: Config, taskConfig: TaskConfig) = init(args)

    val sourceURIsPerSchema: SchemaFiles = FileReader.listFileURIsPerSchema(appConfig.sourceFilesRootPath)
    val databaseTransactionSqls = DBAppModel
      .loadFromSources(sourceURIsPerSchema)
      .addDatabasesAnalysis()(taskConfig)
      .createSQLEntries()
      .map { case (dbName, sqlEntries) => dbName -> sqlEntries.toSql }

    if (appConfig.dryRun) print(databaseTransactionSqls)
    else execute(databaseTransactionSqls)
  }

  private def init(args: Array[String]): (Config, TaskConfig) = {
    val appConfig = OParser.parse(CliParser.parser, args, Config()).getOrThrow(new Exception("Failed to load arguments"))
    val defaultDB = DBProperties.loadProperties(appConfig.dbConnectionPropertiesPath)
    val taskConfig = TaskConfig(DatabaseName(defaultDB.database), Set(defaultDB))
    (appConfig, taskConfig)
  }

  private def execute(databaseTransactionSqls: Map[DatabaseName, SqlsPerTransaction])(implicit taskConfig: TaskConfig): Unit = {
    def executeDatabase(databaseName: DatabaseName, sqls: SqlsPerTransaction): Unit = {
      logger.info(s"Executing against database: ${databaseName.value}")
      val transactionGroups = TransactionGroup.values.toList // going over transaction groups in their logical order
      implicit val dbConnection: DBConnection = taskConfig.dbConnections(databaseName).dbConnection
      transactionGroups.foreach(sqls.get(_).foreach(SqlExecutor.execute(_)))
    }

    databaseTransactionSqls.foreach { case (dbName, entries) => executeDatabase(dbName, entries) }
  }

  private def print(databaseTransactionSqls: Map[DatabaseName, SqlsPerTransaction]): Unit = {
    def printDatabase(databaseName: DatabaseName, sqls: SqlsPerTransaction): Unit = {
      val transactionGroups = TransactionGroup.values.toList // going over transaction groups in their logical order

      val delimiter = "================================================================================"
      val prefix = "= Database: "
      val suffix = "="
      val space = " " * (delimiter.length - prefix.length - databaseName.value.length - suffix.length)
      println(delimiter)
      println(s"$prefix${databaseName.value}$space$suffix")
      println(s"$delimiter\n")
      transactionGroups.foreach(sqls.get(_).foreach(_.foreach(println)))
    }

    databaseTransactionSqls.foreach { case (dbName, entries) => printDatabase(dbName, entries) }
  }

}
