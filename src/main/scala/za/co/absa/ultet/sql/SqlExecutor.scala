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

package za.co.absa.ultet.sql

import com.typesafe.scalalogging.Logger
import za.co.absa.balta.classes.{DBConnection, DBQuerySupport}

object SqlExecutor extends DBQuerySupport {
  private val logger = Logger(getClass.getName)

  def execute(sqls: Seq[String])(implicit dbConnection: DBConnection): Unit = {
    val autoCommitOriginalStatus = dbConnection.connection.getAutoCommit
    dbConnection.connection.setAutoCommit(false)
    var sql: Option[String] = None

    try {
      sqls.foreach { entry =>
        sql = Some(entry)
        runQuery(entry, List.empty) { _ => }
      }
      dbConnection.connection.commit()
    } catch {
      case exception: Exception =>
        dbConnection.connection.rollback()
        sql match {
          case Some(query) => logger.error(s"""Script execution failed at: `$query` with error: "${exception.getMessage}"""")
          case None      => logger.error(s"""Script execution failed with error: "${exception.getMessage}"""")
        }
        throw exception
    }
    finally {
      dbConnection.connection.setAutoCommit(autoCommitOriginalStatus)
    }
  }

  def execute(sql: String)(implicit dbConnection: DBConnection): Unit = {
    execute(Seq(sql))
  }
}
