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

package za.co.absa.ultet.util.parsers.yaml

import cats.syntax.either._
import io.circe.{Error, Json, ParsingFailure}
import io.circe.yaml.{parser => yamlParser}
import io.circe.generic.extras.ConfiguredJsonCodec
import za.co.absa.ultet.model.table.TableDef
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.table.{TableIdentifier, TableName}
import za.co.absa.ultet.types.user.UserName

@ConfiguredJsonCodec
case class DBTableFromYaml(
  table: String,
  description: Option[String],
  primaryDb: String,
  owner: String,
  columns: Seq[ColumnFromYaml] = Seq.empty,
  primaryKey: Option[PrimaryKeyFromYaml] = None,
  indexes: Seq[SecondaryIndexFromYaml] = Seq.empty
) {
  def convertToDBTable(schemaName: SchemaName): TableDef = {
    val tableIdentifier = TableIdentifier(schemaName, TableName(table))
    TableDef(
      tableIdentifier = tableIdentifier,
      primaryDBName = DatabaseName(primaryDb),
      owner = UserName(owner),
      description = description,
      columns = columns.map(_.toTableColumn),
      primaryKey = primaryKey.map(_.toPrimaryKey(tableIdentifier)),
      indexes = indexes.map(_.toSecondaryIndex(tableIdentifier)).toSet
    )
  }
}

object DBTableFromYaml {
  def fromYamlSource(source: String): DBTableFromYaml = {
    val loadedYaml: Either[ParsingFailure, Json] = yamlParser.parse(source)
    loadedYaml
      .leftMap(err => err: Error)
      .flatMap(x => x.as[DBTableFromYaml])
      .valueOr(throw _)
  }
}
