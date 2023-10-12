/*
 * Copyright 2022 ABSA Group Limited
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

import scopt.OParser

object CliParser {
  private val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("Ultet"),
      head("ultet", "0.1"),
      opt[String]('s', "source-files-root-path")
        .action((x, c) => c.copy(sourceFilesRootPath = x))
        .text("Path to the root of source files.")
        .required(),
      opt[Boolean]('d', "dry-run")
        .action((x, c) => c.copy(dryRun = x))
        .text("Dry run option. Does not commit anything to DB.")
        .optional(),
      opt[String]('c', "db-connection-properties")
        .action((x, c) => c.copy(dbConnectionPropertiesPath = x))
        .text("Path to properties file with DB Connection info.")
        .optional(),
      opt[String]('p', "patch-file")
        .action((x, c) => c.copy(patchFilePath = Some(x)))
        .text("Path to the folder where patch files will be saved")
        .optional()
    )
  }
}
