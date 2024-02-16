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

import cats.syntax.either._
import io.circe.generic.auto._
import io.circe.{Error, Json, ParsingFailure, yaml}
import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.model.SchemaName
import za.co.absa.ultet.parsers.helpers.DBTableFromYaml

case class PgTableFileParser(schemaName: SchemaName) extends GenericFileParser[DBTable] {
  override def parseSource(source: String): Set[DBTable] = {
    val processedYaml = processYaml(source)
    Set(processedYaml.convertToDBTable(schemaName))
  }

  private [parsers] def processYaml(source: String): DBTableFromYaml = {
    val loadedYaml: Either[ParsingFailure, Json] = yaml.parser.parse(source)
    loadedYaml
      .leftMap(err => err: Error)
      .flatMap(_.as[DBTableFromYaml])
      .valueOr(throw _)
  }

}
