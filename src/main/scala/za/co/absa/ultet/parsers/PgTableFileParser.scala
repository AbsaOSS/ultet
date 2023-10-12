package za.co.absa.ultet.parsers

import cats.syntax.either._
import io.circe.generic.auto._
import io.circe.{yaml, Error}
import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.dbitems.DBTableMember.{DBTableColumn, DBTableIndex, DBTablePrimaryKey}
import za.co.absa.ultet.model._
import za.co.absa.ultet.model.table.TableName

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.stream.Collectors


case class PgTableFileParser() {
  import PgTableFileParser.DBTableFromYaml

  def parseContentYaml(content: String): DBTableFromYaml = {
    val loadedYaml = yaml.parser.parse(content)
    val processedYaml = loadedYaml
      .leftMap(err => err: Error)
      .flatMap(_.as[DBTableFromYaml])
      .valueOr(throw _)
    processedYaml
  }

  def parseFileYaml(fileUri: URI): DBTable = {
    val path = Paths.get(fileUri)
    val lines = Files.lines(path)
    val content = lines.collect(Collectors.joining("\n"))

    parseContentYaml(content).convertToDBTable
  }

}


object PgTableFileParser {

  case class DBTableFromYaml(
    table: String,
    description: Option[String],
    primaryDBName: String,
    owner: String,
    columns: Seq[Map[String, String]] = Seq.empty,
    primaryKey: Option[Map[String, String]] = None,
    indexes: Seq[Map[String, String]] = Seq.empty
  ) {
    private def prepareColumns: Seq[DBTableColumn] = {
      columns.map(
        currCol => {
          DBTableColumn(
            ColumnName(currCol("columnName")),
            currCol("dataType"),
            currCol("notNull").toBoolean,
            currCol.get("description"),
            currCol.get("default"),
          )
        }
      )
    }

    private def prepareIndexes: Seq[DBTableIndex] = {
      indexes.map(
        currIndex => {
          DBTableIndex(
            currIndex("indexName"),
            currIndex("tableName"),
            currIndex("indexBy")
              .replaceAll("""^\[|\]$""", "")
              .split(",")
              .map(_.trim),
            // todo better all this
            currIndex.getOrElse("unique", "false").toBoolean,
            currIndex.getOrElse("ascendingOrder", "true").toBoolean,
            currIndex.get("nullsFirstOverride").map(_.toBoolean),
            currIndex.getOrElse("nullsDistinct", "true").toBoolean,
          )
        }
      )
    }

    private def preparePrimaryKey: Option[DBTablePrimaryKey] = {
      primaryKey.isDefined match {
        case true =>
          val cols = primaryKey.get("columns")
          val pkName = primaryKey.get("name")

          val preparedPk = DBTablePrimaryKey(
            cols.replaceAll("""^\[|\]$""", "")
              .split(",")
              .map(currColName => ColumnName(currColName.trim)),
            Some(pkName)
          )
          Some(preparedPk)
        case _ => None
      }
    }

    def convertToDBTable: DBTable = {
      val schemaAndTbl = table.split("\\.", 2)
      val semiPreparedTable = DBTable(
        TableName(schemaAndTbl(1)),
        SchemaName(schemaAndTbl(0)),
        description,
        DatabaseName(primaryDBName),
        UserName(owner)
      )

      val withColumns = prepareColumns.foldLeft(semiPreparedTable) { case (acc, preparedColumn) =>
        acc.addColumn(preparedColumn)
      }
      val withColumnsAndIndexes = prepareIndexes.foldLeft(withColumns) { case (acc, preparedIndex) =>
        acc.addIndex(preparedIndex)
      }
      preparePrimaryKey match {
        case Some(pk) => withColumnsAndIndexes.definePrimaryKey(pk)
        case _ => withColumnsAndIndexes
      }
    }
  }
}
