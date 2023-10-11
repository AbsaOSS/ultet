package za.co.absa.ultet.util

case class Config(yamlSource: String = "",
                  dryRun: Boolean = false,
                  dbConnectionPropertiesPath: String = "",
                  patchFilePath: Option[String] = None)
