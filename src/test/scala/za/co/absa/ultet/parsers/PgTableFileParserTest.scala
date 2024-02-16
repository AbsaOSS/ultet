package za.co.absa.ultet.parsers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.dbitems.DBTable
import za.co.absa.ultet.dbitems.table.DBTableColumn
import za.co.absa.ultet.dbitems.table.DBTableIndex.{DBPrimaryKey, DBSecondaryIndex, IndexColumn}
import za.co.absa.ultet.model._
import za.co.absa.ultet.model.table.{ColumnName, IndexName, TableIdentifier, TableName}
import za.co.absa.ultet.parsers.helpers.DBTableFromYaml

class PgTableFileParserTest extends AnyFlatSpec with Matchers {

  private val schemaName = SchemaName("testSchema")

  "PgTableFileParserTest" should "return semi-prepared object table from example content" in {
    val tableString =
      """table: testTable
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
        |    indexBy: "[column1]"
        |""".stripMargin

    PgTableFileParser(schemaName).processYaml(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
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
        "indexBy" -> "[column1]"  // TODO, horrible! Must be string in the YAML file!
      ))
    )
  }

  "PgTableFileParserTest" should "return well-prepared table object from example content" in {
    val tableString =
      """table: testTable
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
        |    indexBy: "[column1]"
        |""".stripMargin

    PgTableFileParser(schemaName).parseSource(tableString).head shouldBe DBTable(
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
      primaryKey = Some(DBPrimaryKey(
        tableIdentifier = TableIdentifier(SchemaName("testSchema"), TableName("testTable")),
        columns = Seq("id_key_field1", "id_key_field1").map(IndexColumn(_)),
        indexName = IndexName("pk_my_table")
      )),
      indexes = Set(DBSecondaryIndex(
        tableIdentifier = TableIdentifier(SchemaName("testSchema"), TableName("testTable")),
        indexName = IndexName("idx_some_name"),
        columns = Seq("column1").map(IndexColumn(_))
      ))
    )
  }

  "PgTableFileParserTest" should "return semi-prepared object table from example content, some attributes empty" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns: []
        |primaryKey:
        |indexes: []
        |""".stripMargin

    PgTableFileParser(schemaName).processYaml(tableString) shouldBe DBTableFromYaml(
      table = "testTable",
      description = Some("Some Description of this madness"),
      primaryDBName = "primaryDB",
      owner = "some_owner_user",
    )
  }

  "PgTableFileParserTest" should "return well-prepared object table from example content, some attributes empty" in {
    val tableString =
      """table: testTable
        |description: Some Description of this madness
        |primaryDBName: primaryDB
        |owner: some_owner_user
        |columns: []
        |primaryKey:
        |indexes: []
        |""".stripMargin

    PgTableFileParser(schemaName).parseSource(tableString).head shouldBe DBTable(
      tableName = TableName("testTable"),
      schemaName = SchemaName("testSchema"),
      description = Some("Some Description of this madness"),
      primaryDBName = DatabaseName("primaryDB"),
      owner = UserName("some_owner_user"),
    )
  }
}
