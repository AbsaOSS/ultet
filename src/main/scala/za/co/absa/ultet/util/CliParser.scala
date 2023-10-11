package za.co.absa.ultet.util

import scopt.OParser

case class Config(yamlSource: String = "",
                  dryRun: Boolean = false,
                  dbConnectionPropertiesPath: String = "",
                  patchFilePath: Option[String] = None)

object CliParser {
  private val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("Ultet"),
      head("ultet", "0.1"),
      opt[String]('s', "source-yaml")
        .action((x, c) => c.copy(yamlSource = x))
        .text("Path to source file(s). Use * as a wildcard.")
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
