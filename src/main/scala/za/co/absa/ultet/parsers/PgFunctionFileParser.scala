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
package za.co.absa.ultet.parsers

import za.co.absa.ultet.dbitems.DBFunctionFromSource

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors

case class PgFunctionFileParser() {

  def parseFile(fileUri: URI): DBFunctionFromSource = {
    val path = Paths.get(fileUri)

    val lines = Files.lines(path)
    val content = lines.collect(Collectors.joining("\n"))

    parseString(content)
  }

  def parseString(str: String): DBFunctionFromSource = {
    import PgFunctionFileParser._

    val owner = ownerRx.findAllMatchIn(str).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse owner from $str")
      case o :: Nil => o.group(1)
      case _ => throw new IllegalStateException(s"Found more than 2 owners in $str")
    }

    val (databaseName, users) = dbUsersRx.findAllMatchIn(str).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse database or users from $str")
      case line :: Nil => {
        val dbName = line.group(1)
        val unparsedUsers = line.group(2)
        val trimmedUsers = unparsedUsers.split(',').map(_.trim).filter(_.nonEmpty)
        if (trimmedUsers.isEmpty) {
          throw new IllegalStateException(s"Could not parse users name from $str")
        } else {
          (dbName, trimmedUsers)
        }
      }
      case _ => throw new IllegalStateException(s"Found more than 2 database records in $str")
    }

    val (schemaName, fnName, inParamTypes) = schemaFnParamsRx.findAllMatchIn(str).toList match {
      case Nil => throw new IllegalStateException(s"Could not parse schemaName, fn name or params from $str")
      case line :: Nil => {
        val schema = line.group(1)
        val fn = line.group(2)
        val paramsStr = line.group(3)

        val paramsUnparsed = paramsStr.split(',').map(_.trim).toSeq
        val optParamTypes: Seq[Option[String]] = paramsUnparsed.map { param =>
          param match {
            case singleParamCapturingRx(inOut, _, paramType) =>
              if (inOut.toUpperCase == "IN") Some(paramType) else None

            case _ => throw new IllegalStateException(s"Could not parameter from $param")
          }
        }
        (schema, fn, optParamTypes.flatten) // just keep the IN paramTypes
      }
      case _ => throw new IllegalStateException(s"Found more than 2 create function mentions in $str")
    }

    DBFunctionFromSource(fnName, inParamTypes, owner, users, schemaName, databaseName, str)
  }
}

object PgFunctionFileParser {

  val ownerRx = """--\s*owner:\s*([_a-zA-Z0-9]+)\s*""".r
  val dbUsersRx = """--\s*database:\s*([_a-zA-Z0-9]+)\s*\((\s*(?:[_a-zA-Z0-9]+)\s*(?:,\s*[_a-zA-Z0-9]+\s*)*\s*)\)""".r // need to ,-break and trim the users
  val schemaFnParamsRx = """(?i)CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([_a-zA-Z0-9]+)\.([_a-zA-Z0-9]+)\s*\(([,\s_a-zA-Z0-9]+)\)""".r // need to break params

  val singleParamCapturingRx = """(?i)\s*(IN|OUT)\s+([_a-zA-Z0-9]+)\s+([_a-zA-Z0-9]+)\s*""".r // used to break up params from ^^

}
