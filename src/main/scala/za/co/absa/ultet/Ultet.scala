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
import za.co.absa.ultet.dbitems.DBItem
import za.co.absa.ultet.model.{SQLEntry, SchemaName, TransactionGroup}
import za.co.absa.ultet.util.{CliParser, Config, DBProperties}

import java.net.URI
import java.nio.file._
import scala.collection.JavaConverters._
import java.sql.{Connection, DriverManager, ResultSet}
import scala.util.{Failure, Success, Try}

object Ultet {
  private val logger = Logger(getClass.getName)

  private def extractSQLEntries(dbItems: Set[DBItem]): Seq[SQLEntry] = {
    dbItems.toSeq.flatMap { item => item.sqlEntries }
  }

  private def runEntries(entries: Seq[SQLEntry], dryRun: Boolean = false)(implicit connection: Connection): Unit = {
    if(dryRun) entries.map(_.sqlExpression).foreach(println)
    else {
      val resultSets: Seq[ResultSet] = runTransaction(connection, entries)

      for (resultSet <- resultSets) {
        val numColumns = resultSet.getMetaData.getColumnCount
        while (resultSet.next()) {
          val row = (1 to numColumns).map(col => resultSet.getString(col))
          logger.info(row.mkString(", "))
        }
        resultSet.close()
      }
    }
  }

  def runTransaction(connection: Connection, entries: Seq[SQLEntry]): Seq[ResultSet] = {
    val autoCommitOriginalStatus = connection.getAutoCommit
    connection.setAutoCommit(false)

    val resultSets = Try {
      entries.foldLeft(List[ResultSet]()) { case (acc, entry) =>
        val statement = connection.createStatement()
        statement.execute(entry.sqlExpression)
        val ret: List[ResultSet] = acc :+ statement.getResultSet
        statement.close()
        ret
      }
    } match {
      case Success(resultSets) =>
        connection.commit()
        resultSets
      case Failure(exception) =>
        connection.rollback()
        connection.close()
        throw new Exception("Script execution failed", exception)
    }

    connection.setAutoCommit(autoCommitOriginalStatus)
    resultSets
  }

  def sortEntries(entries: Seq[SQLEntry]): Map[TransactionGroup.Value, Seq[SQLEntry]] = {
    entries
      .groupBy(_.transactionGroup)
      .mapValues(_.sortBy(_.orderInTransaction))
  }

  private def listChildPaths(path: Path): List[Path] = Files.list(path)
    .iterator()
    .asScala
    .toList

  def listFileURIsPerSchema(pathString: String): Map[SchemaName, List[URI]] = {
    val path = Paths.get(pathString)
    val schemaPaths = listChildPaths(path)
    schemaPaths
      .map(p => SchemaName(p.getFileName.toString) -> listChildPaths(p))
      .toMap
      .mapValues(_.map(_.toUri))
  }

  def main(args: Array[String]): Unit = {
    val config = OParser.parse(CliParser.parser, args, Config()) match {
      case Some(config) => config
      case _ => throw new Exception("Failed to load arguments")
    }

    val dbProperties = DBProperties.loadProperties(config.dbConnectionPropertiesPath)
    val dbPropertiesSys = DBProperties.getSysDB(dbProperties)
    val dbConnection: String = dbProperties.generateConnectionString()
    implicit val jdbcConnection: Connection = DriverManager.getConnection(
      dbConnection, dbProperties.user, dbProperties.password
    )

    val sourceURIsPerSchema: Map[SchemaName, List[URI]] = listFileURIsPerSchema(config.sourceFilesRootPath)

    val dbItems: Set[DBItem] = DBItem.createDBItems(sourceURIsPerSchema)
    val entries: Seq[SQLEntry] = extractSQLEntries(dbItems)
    val orderedEntries = sortEntries(entries)
    val databaseEntries = orderedEntries.getOrElse(TransactionGroup.Databases, Seq.empty)
    val roleEntries = orderedEntries.getOrElse(TransactionGroup.Roles, Seq.empty)
    val objectEntries = orderedEntries.getOrElse(TransactionGroup.Objects, Seq.empty)
    val indexEntries = orderedEntries.getOrElse(TransactionGroup.Indexes, Seq.empty)

    if (databaseEntries.nonEmpty) runEntries(databaseEntries, config.dryRun)
    if (roleEntries.nonEmpty) runEntries(roleEntries, config.dryRun)
    if (objectEntries.nonEmpty) runEntries(objectEntries, config.dryRun)
    if (indexEntries.nonEmpty) runEntries(indexEntries, config.dryRun)

    jdbcConnection.close()
  }
}
