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
import za.co.absa.ultet.model.{SQLEntry, TransactionGroup}
import za.co.absa.ultet.util.{CliParser, Config, DBProperties}

import java.nio.file._
import scala.collection.JavaConverters._
import java.sql.{Connection, DriverManager, ResultSet}
import scala.util.{Failure, Success, Try}

object Ultet {
  private val logger = Logger(getClass.getName)

  private def runEntries(dbProperties: DBProperties, dbConnection: String, entries: Seq[SQLEntry]): Unit = {
    val connection: Connection = DriverManager.getConnection(dbConnection, dbProperties.user, dbProperties.password)
    val resultSets: Seq[ResultSet] = runTransaction(connection, entries)
    connection.close()

    for (resultSet <- resultSets) {
      val numColumns = resultSet.getMetaData.getColumnCount
      while (resultSet.next()) {
        val row = (1 to numColumns).map(col => resultSet.getString(col))
        logger.info(row.mkString(", "))
      }
      resultSet.close()
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

  def listFiles(pathString: String): List[Path] = {
    val path = Paths.get(pathString)
    val directory = path.getParent
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:${path.getFileName}")

    Files.list(directory)
      .iterator()
      .asScala
      .filter(x => matcher.matches(x.getFileName))
      .toList
  }

  def main(args: Array[String]): Unit = {
    val config = OParser.parse(CliParser.parser, args, Config()) match {
      case Some(config) => config
      case _ => throw new Exception("Failed to load arguments")
    }

    val dbProperties = DBProperties.loadProperties(config.dbConnectionPropertiesPath)
    val dbPropertiesSys = DBProperties.getSysDB(dbProperties)
    val dbConnection: String = dbProperties.generateConnectionString()
    val yamls: List[Path] = listFiles(config.yamlSource)

    println(dbConnection)
    yamls.foreach(x => println(x.toString))

    val entries: Seq[SQLEntry] = Seq.empty // TODO
    val orderedEntries = sortEntries(entries)
    val databaseEntrries = orderedEntries.getOrElse(TransactionGroup.Databases, Seq.empty)
    val roleEntrries = orderedEntries.getOrElse(TransactionGroup.Roles, Seq.empty)
    val objectEntrries = orderedEntries.getOrElse(TransactionGroup.Objects, Seq.empty)
    val indexEntrries = orderedEntries.getOrElse(TransactionGroup.Indexes, Seq.empty)

    if (databaseEntrries.nonEmpty) runEntries(dbPropertiesSys, dbConnection, databaseEntrries)
    if (roleEntrries.nonEmpty) runEntries(dbProperties, dbConnection, roleEntrries)
    if (objectEntrries.nonEmpty) runEntries(dbProperties, dbConnection, objectEntrries)
    if (indexEntrries.nonEmpty) runEntries(dbProperties, dbConnection, indexEntrries)
  }
}
