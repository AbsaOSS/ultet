package za.co.absa.ultet.parsers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.dbitems.DBTableMember.{DBTableColumn, DBTableIndex, DBTablePrimaryKey}
import za.co.absa.ultet.model._
import za.co.absa.ultet.parsers.PgTableFileParser.DBTableFromYaml

class PgTableFileParserTest extends AnyFlatSpec with Matchers {

  "PgTableFileParserTest" should "return well-prepared object table from example content" in {
    val tableString =
      """tableName: testTable
        |schemaName: testSchema
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns:
        |  - columnName: column1
        |    dataType: bigint
        |    notNull: "true"
        |primaryKey:
        |    name: pk_my_table
        |    columns: "[id_key_field1, id_key_field1]"
        |indexes:
        |  - indexName: idx_some_name
        |    tableName: testTable
        |    indexBy: column1
        |""".stripMargin

    PgTableFileParser().parseContentYaml(tableString) shouldBe DBTableFromYaml(
      tableName = "testTable",
      schemaName = "testSchema",
      description = Some("Some Description of this madness"),
      primaryDBName = "primaryDB",
      owner = "some_owner_user",
      columns = Seq(
        Map(
          "columnName" -> "column1",
          "dataType" -> "bigint",
          "notNull" -> "true"  // TODO, horrible! Must be string in the YAML file!
        ),
      ),
      primaryKey = Some(Map(
        "name" -> "pk_my_table",
        "columns" -> "[id_key_field1, id_key_field1]"  // TODO, horrible! Must be string in the YAML file!
      )),
      indexes = Seq(Map(
        "indexName" -> "idx_some_name",
        "tableName" -> "testTable",
        "indexBy" -> "column1"
      ))
    )
  }

  "PgTableFileParserTest" should "parse table from example content" in {
    val tableString =
      """tableName: testTable
        |schemaName: testSchema
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns:
        |  - columnName: column1
        |    dataType: bigint
        |    notNull: "true"
        |primaryKey:
        |    name: pk_my_table
        |    columns: "[id_key_field1, id_key_field1]"
        |indexes:
        |  - indexName: idx_some_name
        |    tableName: testTable
        |    indexBy: column1
        |""".stripMargin

    PgTableFileParser().parseContentYaml(tableString).convertToDBTable shouldBe DBTable(
      tableName = TableName("testTable"),
      schemaName = SchemaName("testSchema"),
      description = Some("Some Description of this madness"),
      primaryDBName = DatabaseName("primaryDB"),
      owner = UserName("some_owner_user"),
      columns = Seq(
        DBTableColumn(
          columnName = ColumnName("column1"),
          dataType = "bigint",
          notNull = true
        ),
      ),
      primaryKey = Some(DBTablePrimaryKey(
        columns = Seq(ColumnName("id_key_field1"), ColumnName("id_key_field1")),
        name = Some("pk_my_table"),
      )),
      indexes = Seq(DBTableIndex(
        indexName = "idx_some_name",
        tableName = "testTable",
        indexBy = Seq("column1")
      ))
    )
  }
}
