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

import java.io.{BufferedWriter, File, FileWriter => JavaFileWriter}
import java.net.URI
import java.nio.file.{Files, Path}

trait FileWriter {
  def writeFile(filename: URI, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new JavaFileWriter(file))
    for (line <- lines) {
      bw.write(s"$line\n")
    }
    bw.close()
  }

  /**
   * write a `String` to the `filename`.
   */
  def writeFile(filename: URI, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new JavaFileWriter(file))
    bw.write(s)
    bw.close()
  }


  def createTempFile(prefix: Option[String], suffix: Option[String], deleteOnExit: Boolean = true): URI = {

    val path: Path = Files.createTempFile(prefix.orNull, suffix.orNull)
    if (deleteOnExit) {
      path.toFile.deleteOnExit()
    }
    path.toUri
  }
}
