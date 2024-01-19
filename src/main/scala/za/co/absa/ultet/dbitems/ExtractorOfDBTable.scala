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

import za.co.absa.balta.classes.setter.Params
import za.co.absa.balta.classes.{DBConnection, DBQuerySupport, QueryResult, QueryResultRow}
import za.co.absa.ultet.dbitems.DBTableMember.{DBTableColumn, DBTableIndex, DBTablePrimaryKey}
import za.co.absa.ultet.model.SchemaName
import za.co.absa.ultet.model.table.{ColumnName, TableName}

import java.sql.{Connection, ResultSet}

class ExtractorOfDBTable(val schemaName: SchemaName, val tableName: TableName)
                        (implicit jdbcConnection: Connection) extends DBQuerySupport {

  private val ownerQuery =
    s"""
       |SELECT tableowner
       |FROM pg_tables
       |WHERE
       |  lower(schemaname) = '${schemaName.normalized}' AND
       |  lower(tablename) = '${tableName.normalized}'
       |""".stripMargin
  private val columnsQuery =
    s"""
       |SELECT
       |  C.column_name, C.data_type, C.is_nullable, C.column_default,
       |  PGD.description
       |FROM
       |    pg_catalog.pg_statio_all_tables AS ST
       |  INNER JOIN pg_catalog.pg_description PGD ON
       |    pgd.objoid = st.relid
       |  INNER JOIN information_schema.columns C ON
       |    PGD.objsubid   = C.ordinal_position AND
       |    C.table_schema = ST.schemaname AND
       |    C.table_name   = ST.relname
       |WHERE
       |  lower(C.table_schema) = '${schemaName.normalized}' AND
       |  lower(C.table_name) = '${tableName.normalized}'
       |""".stripMargin

  private val descriptionQuery =
    s"""
       |SELECT PGD.description
       |FROM pg_catalog.pg_statio_all_tables AS ST
       |  INNER JOIN pg_catalog.pg_description PGD ON
       |    PGD.objoid = ST.relid
       |WHERE
       |  pgd.objsubid = 0 AND
       |  lower(ST.schemaname) = ${schemaName.normalized} AND
       |  lower(ST.relname) = ${tableName.normalized}
       |""".stripMargin

  private val indexesQuery =
    s"""
      |""".stripMargin

  def owner: Option[String] = {
    val preparedStatement = jdbcConnection.prepareStatement(ownerQuery)
    val result = preparedStatement.executeQuery()
    if (result.next()) {
      Some(result.getString("tableowner"))
    } else {
      None
    }
  }
  def description: Option[String] = {
    val preparedStatement = jdbcConnection.prepareStatement(descriptionQuery)
    val result = preparedStatement.executeQuery()
    if (result.next()) {
      Some(result.getString("tableowner"))
    } else {
      None
    }
  }

  def columns: Seq[DBTableColumn] = {
    def resultToColumn(currentRow: ResultSet): DBTableColumn = {
      DBTableColumn(
        columnName = ColumnName(currentRow.getString("column_name")),
        dataType = currentRow.getString("data_type"), //TODO  Type parsing #88
        notNull = currentRow.getString("is_nullable") == "NO",
        description = Option(currentRow.getString("description")),
        default = Option(currentRow.getString("column_default"))
      )
    }

    val preparedStatement = jdbcConnection.prepareStatement(columnsQuery)
    val result = preparedStatement.executeQuery()
    val seqBuilder = Seq.newBuilder[DBTableColumn]

    while (result.next()) seqBuilder += resultToColumn(result)

    seqBuilder.result()
  }

  def primaryKey: Option[DBTablePrimaryKey] = {
    // TODO #94
    None
  }

  def indexes: Seq[DBTableIndex] = {
    // TODO #94
    Seq.empty
  }

  private implicit val connection: DBConnection = new DBConnection(jdbcConnection)
  private val setters = Params.add(schemaName.normalized).add(tableName.normalized).setters

  private case class IndexColumnRow (
                                    indexName: String,
                                    indexBy: Seq[IndexField],
                                    description: Option[String] = None,
                                    unique: Boolean = false,
                                    nullsDistinct: Boolean = true,
                                    constraint: Option[String] = None
                                    )
  private object IndexColumnRow {
    def apply(row: QueryResultRow): IndexColumnRow = new IndexColumnRow(
      indexName = row.getString("index_name"),
      tableName = row.getString("table_name"),
      indexBy = Seq.empty,
      description = Option(row.getString("description")),
      unique = row.getString("is_unique") == "YES",
      nullsDistinct = row.getString("nulls_distinct") == "YES",
      constraint = Option(row.getString("constraint_name"))
  }
  private def xxx: (Seq[DBTableIndex], Option[DBTablePrimaryKey]) = {
    val indexes = runQuery("", setters)(_.toList).map(IndexColumnRow(_)).groupBy(_.indexName)
    ???
  }

  private def decodeIndexRow(row: QueryResultRow): Either[DBTablePrimaryKey, DBTableIndex] = {
    if (row.getBoolean("") {
      Left(DBTablePrimaryKey(
        columns = Seq.empty,
        name = Option(row.getString("constraint_name"))
      ))
    } else {
      Right(DBTableIndex(
        indexName = row.getString("index_name"),
        tableName = row.getString("table_name"),
        indexBy = Seq.empty,
        description = Option(row.getString("description")),
        unique = row.getString("is_unique") == "YES",
        nullsDistinct = row.getString("nulls_distinct") == "YES",
        constraint = Option(row.getString("constraint_name"))
      ))
    }
  }

}
