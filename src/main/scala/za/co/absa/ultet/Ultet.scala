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

import scopt.OParser
import za.co.absa.ultet.util.{CliParser, Config, DBProperties}

import java.nio.file._
import scala.collection.JavaConverters._

object Ultet {
  def listFiles(pathString: String): List[Path] = {
    val path = Paths.get(pathString)
    val directory = path.getParent
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:*${path.getFileName}")

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

    val dbConnection: String = DBProperties
      .loadProperties(config.dbConnectionPropertiesPath)
      .generateConnectionString()
    val yamls: List[Path] = listFiles(config.yamlSource)

    println(dbConnection)
    yamls.foreach(x => println(x.toString))

  }
}
