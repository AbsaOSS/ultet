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

package za.co.absa.ultet.util

import za.co.absa.ultet.model.SchemaName
import za.co.absa.ultet.util.SourceFileType.{FunctionSrc, SourceFileType}

import java.net.URI
import java.nio.file._
import java.util.stream.Collectors
import scala.collection.JavaConverters.asScalaIteratorConverter

object FileReader {

  type SchemaFiles = Map[SchemaName, Set[URI]]

  def readFileAsString(fileUri: URI): String = {
    val path = Paths.get(fileUri)

    val lines = Files.lines(path)
    lines.collect(Collectors.joining("\n"))
  }

  def listFileURIsPerSchema(pathString: String): SchemaFiles = {
    val path = Paths.get(pathString)
    val schemaPaths = listChildPaths(path)
    schemaPaths
      .map(p => SchemaName(p.getFileName.toString) -> listChildPaths(p))
      .toMap
      .mapValues(_.map(_.toUri))
  }

  def fileType(uri: URI): SourceFileType = {
    val path = uri.getPath
    if (path.endsWith(".sql")) {
      SourceFileType.FunctionSrc
    } else if (path.endsWith(".yml")) {
      SourceFileType.TableSrc
    } else if (path == "owner.txt") {
      SourceFileType.SchemaOwner
    } else {
      SourceFileType.Unknown
    }
  }

  private def listChildPaths(path: Path): Set[Path] = Files.list(path)
    .iterator()
    .asScala
    .toSet
}
