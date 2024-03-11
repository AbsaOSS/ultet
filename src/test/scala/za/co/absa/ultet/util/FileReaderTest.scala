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

import org.scalatest.funsuite.AnyFunSuiteLike
import za.co.absa.ultet.FileWriter
import za.co.absa.ultet.types.schema.SchemaName

import java.net.URI
import java.nio.file.Paths

class FileReaderTest extends AnyFunSuiteLike with FileWriter {

  test("Reading a file with and without trimming") {
    val tempFileName = createTempFile(None, None)
    writeFile(tempFileName, " This is a test file ")
    val resultAsIs = FileReader.readFileAsString(tempFileName)
    assert(resultAsIs == " This is a test file ")
    val resultTrimmed = FileReader.readFileAsString(tempFileName, trim = true)
    assert(resultTrimmed == "This is a test file")
  }

  test("Reading a a multiline file") {
    val tempFileName = createTempFile(None, None)
    writeFile(tempFileName, Seq("Hello", "world!"))
    val result = FileReader.readFileAsString(tempFileName)
    assert(result == "Hello\nworld!")
  }

  test("List example files") {
    val expected = Map(SchemaName("my_schema") -> Set(
      Paths.get("examples/database/src/main/my_schema/_internal_function.sql").toUri,
      Paths.get("examples/database/src/main/my_schema/dbowner.txt").toUri,
      Paths.get("examples/database/src/main/my_schema/my_table.yaml").toUri,
      Paths.get("examples/database/src/main/my_schema/public_function.sql").toUri
    ))
    val result = FileReader.listFileURIsPerSchema("examples/database/src/main")
    assert(result == expected)
  }

  test("Check file types") {
    assert(FileReader.fileType(new URI("file:///tmp/foo.sql")) == SourceFileType.FunctionSrc)
    assert(FileReader.fileType(new URI("file:///tmp/bar.yml")) == SourceFileType.TableSrc)
    assert(FileReader.fileType(new URI("file:///tmp/bar.yaml")) == SourceFileType.TableSrc)
    assert(FileReader.fileType(new URI("file:///tmp/owner.txt")) == SourceFileType.SchemaOwner)
    assert(FileReader.fileType(new URI("file:///tmp/foo.bar")) == SourceFileType.Unknown)
  }

}
