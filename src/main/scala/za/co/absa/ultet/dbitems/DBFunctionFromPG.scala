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


import za.co.absa.ultet.model.function.{FunctionArgumentType, FunctionDrop, FunctionName}
import za.co.absa.ultet.model.{SQLEntry, SchemaName}

import java.sql.Connection

case class DBFunctionFromPG(
                             schema: SchemaName,
                             fnName: FunctionName,
                             paramTypes: Seq[FunctionArgumentType]
                           ) extends DBFunction {
  override def sqlEntries: Seq[SQLEntry] = Seq(FunctionDrop(schema, fnName, paramTypes))
}

object DBFunctionFromPG {

  def fetchAllOfSchema(schemaName: SchemaName)
                      (implicit jdbcConnection: Connection): Seq[DBFunctionFromPG] = {
    val query =
      s"""
         |SELECT
         |  p.proname AS fn_name,
         |  pg_catalog.pg_get_function_arguments(p.oid) AS in_and_out_arguments
         |FROM
         |  pg_catalog.pg_namespace n JOIN
         |  pg_catalog.pg_proc p ON p.pronamespace = n.oid
         |WHERE
         |  n.nspname = '${schemaName.value}' AND
         |  p.prokind = 'f';
         |""".stripMargin

    fetchAllOfSchemaWithQuery(schemaName, query)
  }

  def fetchAllOverloads(schemaName: SchemaName, functionName: FunctionName)
                       (implicit jdbcConnection: Connection): Seq[DBFunctionFromPG] = {
    val query =
      s"""
         |SELECT
         |  p.proname AS fn_name,
         |  pg_catalog.pg_get_function_arguments(p.oid) AS in_and_out_arguments
         |FROM
         |  pg_catalog.pg_namespace n JOIN
         |  pg_catalog.pg_proc p ON p.pronamespace = n.oid
         |WHERE
         |  n.nspname = '${schemaName.value}' AND
         |  p.proname = '${functionName.value}' AND
         |  p.prokind = 'f';
         |""".stripMargin

    fetchAllOfSchemaWithQuery(schemaName, query)
  }

  private def fetchAllOfSchemaWithQuery(schemaName: SchemaName, query: String)
                                       (implicit jdbcConnection: Connection): Seq[DBFunctionFromPG] = {
    val preparedStatement = jdbcConnection.prepareStatement(query)
    val result = preparedStatement.executeQuery()
    val seqBuilder = Seq.newBuilder[DBFunctionFromPG]

    while (result.next()) {
      val fnName = result.getString("fn_name")
      val inAndOutArguments = result.getString("in_and_out_arguments")
      val inArguments = inAndOutArguments
        .split(",")
        .map(_.trim)
        .filterNot(_.toUpperCase.startsWith("OUT"))
      val argumentTypes = inArguments
        .map(_.split(" ")(1))
        .map(FunctionArgumentType)

      val dbFunctionFromPG = DBFunctionFromPG(
        schemaName,
        FunctionName(fnName),
        argumentTypes
      )
      seqBuilder += dbFunctionFromPG
    }

    seqBuilder.result()
  }

}
