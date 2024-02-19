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

package za.co.absa.ultet.util.parsers

import za.co.absa.ultet.model.function.FunctionFromSource
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.function.{FunctionArgumentType, FunctionName}
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.user.UserName

object PgFunctionFileParser extends GenericFileParser[FunctionFromSource] {

  override def parseSource(source: String): Set[FunctionFromSource] = {
    val owner = parseOwnerFromSql(source)
    val (databaseName, users) = parseDatabaseNameAndUsersFromSql(source)
    val (schemaName, fnName, inParamTypes) = parseSchemaNameFnNameAndInParamTypesFromSql(source)

    Set(FunctionFromSource(
      FunctionName(fnName),
      inParamTypes.map(FunctionArgumentType),
      UserName(owner),
      users.map(UserName).toSet,
      SchemaName(schemaName),
      DatabaseName(databaseName),
      source
    ))
  }

  private def parseOwnerFromSql(sqlStr: String): String = {
    ownerRx.findAllMatchIn(sqlStr).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse owner from $sqlStr")
      case o :: Nil => o.group(1)
      case _ => throw new IllegalStateException(s"Found more than 2 owners in $sqlStr")
    }
  }

  private def parseDatabaseNameAndUsersFromSql(sqlStr: String): (String, Seq[String]) = {
    dbUsersRx.findAllMatchIn(sqlStr).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse database or users from $sqlStr")
      case line :: Nil =>
        val dbName = line.group(1)
        // there may be no users at all, so the group #2 may not exist
        val trimmedUsers = Option(line.group(2)) match {
          case None => Seq.empty
          case Some(unparsedUsers) =>
            unparsedUsers.split(',').map(_.trim).filter(_.nonEmpty).toSeq
        }

        (dbName, trimmedUsers)
      case _ => throw new IllegalStateException(s"Found more than 2 database records in $sqlStr")
    }
  }

  private def parseSchemaNameFnNameAndInParamTypesFromSql(sqlStr: String): (String, String, Seq[String]) = {
    schemaFnParamsRx.findAllMatchIn(sqlStr).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse schemaName, fn name or params from $sqlStr")
      case line :: Nil =>
        val schema = line.group(1)
        val fn = line.group(2)
        val paramsStr = line.group(3)

        val paramsUnparsed = paramsStr.split(',').map(_.trim).toSeq
        val optParamTypes: Seq[Option[String]] = paramsUnparsed.map {
          case singleParamCapturingRx(inOut, _, paramType) =>
            if (inOut.toUpperCase == "IN") Some(paramType) else None

          case param => throw new IllegalStateException(s"Could not parameter from $param")
        }
        (schema, fn, optParamTypes.flatten) // just keep the IN paramTypes
      case _ => throw new IllegalStateException(s"Found more than 2 create function mentions in $sqlStr")
    }
  }

  private val ownerRx = """--\s*owner:\s*(\w+)\s*""".r
  //                              1--1
  // 1 - capturing group for owner

  private val dbUsersRx = """--\s*database:\s*(\w+)\s*\(\s*((?:\w+)\s*(?:,\s*\w+\s*)*\s*)?\)""".r // need to ,-break and trim the users
  //                                  1---1   2    34-----4   5------------5    6 7
  // 1 - capturing group #1 for db-name
  // 2,7 verbatim "()" in which users are written
  // 3,6 - capturing group #2 for list of users. Made optional by the ? (above "6")
  // 4 - non-capturing group for the first user
  // 5 - other users - comma separated from the first user


  private val schemaFnParamsRx = """(?i)CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+(\w+)\.(\w+)\s*\(([,\s\w]+)\)""".r // need to break params
  //                        1--1       2-----------------2              3---3 45---5    6 7-------7 8
  // 1 - case insensitive matching
  // 2 - non-capturing group of optionally present " OR REPLACE"
  // 3 - capturing-group #1 schema name
  // 4 - verbatim "." separates schema from fnName
  // 5 - capturing-group #2
  // 6,8 - verbatim "()" in which parameters are written
  // 7 - capturing-group #3 - parameters are matched there as a block - \s also covers line-breaks => parsed further later


  private val singleParamCapturingRx = """(?i)\s*(IN|OUT)\s+(\w+)\s+(\w+)\s*""".r // used to break up params from ^^
  //                              1--1   2------2   3---3   4---4
  // 1 - case insensitive matching
  // 2 - capturing-group #1 IN/OUT param
  // 3 - capturing-group #2 for param name
  // 4 - capturing-group #3 for param type

}
