package za.co.absa.ultet.parsers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.dbitems.DBFunctionFromSource

class PgFunctionFileParserTest extends AnyFlatSpec with Matchers {

  "PgFunctionFileParser" should "parse function from example content" in {
    val functionString =
    """/* comment here and there **/
      |-- owner:
      |     owner_user123
      |-- database: eXample_db (
      |     user_for_accessA, user_for_accessB  ,   user_for_accessZ
      |)
      |CREATE or REPLACE FUNCTION my_schema1.public_functionX(
      |    in  i_parameter         TEXT,
      |    IN  i_parameter2        INTEGER,
      |    OUT status              INTEGER,
      |    IN  i_parameter3        hstore,
      |    OUT status_text         TEXT
      |) RETURNS record AS
      |$$
      |-------------------------------------------------------------------------------
      |--
      |-- Function: my_schema.public_function([Function_Param_Count])
      |--      [Descrip""".stripMargin

    PgFunctionFileParser().parseString(functionString) shouldBe DBFunctionFromSource(
      fnName = "public_functionX",
      paramTypes = Seq("TEXT", "INTEGER", "hstore"),
      owner = "owner_user123",
      users = Seq("user_for_accessA", "user_for_accessB", "user_for_accessZ"),
      schema = "my_schema1",
      database = "eXample_db",
      sqlBody = functionString // the whole thing
    )

  }

  it should "parse example function example file" in {
   val testFileUri = getClass().getClassLoader().getResource("public_function_example.sql").toURI

    val parsedDBItemFromSource = PgFunctionFileParser().parseFile(testFileUri)

    parsedDBItemFromSource.fnName shouldBe "public_function"
    parsedDBItemFromSource.schema shouldBe "my_schema"
    parsedDBItemFromSource.paramTypes shouldBe Seq("TEXT")
    parsedDBItemFromSource.owner shouldBe "some_owner_user"
    parsedDBItemFromSource.users shouldBe Seq("user_for_access")
    parsedDBItemFromSource.database shouldBe "example_db"
    parsedDBItemFromSource.sqlBody should include("CREATE OR REPLACE FUNCTION my_schema.public_function")
  }
}
