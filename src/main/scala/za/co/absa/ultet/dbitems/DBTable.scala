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
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName, TableName}
import za.co.absa.ultet.model.{DatabaseName, SchemaName, UserName, TableName}

import java.sql.Connection

//TODO checks on validity of entries
case class DBTable(
                   tableName: TableName,
                   schemaName: SchemaName,
                   primaryDBName: DatabaseName,
                   owner: UserName,
                   description: Option[String] = None,
                   columns: Seq[DBTableColumn] = Seq.empty,
                   primaryKey: Option[DBTablePrimaryKey] = None,
                   indexes: Seq[DBTableIndex] = Seq.empty
                   ) {
  def addColumn(column: DBTableColumn): DBTable = {
    this.copy(columns = columns ++ Seq(column) )
  }
  def addIndex(index: DBTableIndex): DBTable = {
    this.copy(indexes = indexes ++ Seq(index))
  }
  def definePrimaryKey(pk: DBTablePrimaryKey): DBTable = {
    this.copy(primaryKey = Some(pk))
  }
}

object DBTable {
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
