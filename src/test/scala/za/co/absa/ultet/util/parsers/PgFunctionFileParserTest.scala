package za.co.absa.ultet.util.parsers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.ultet.model.function.FunctionFromSource
import za.co.absa.ultet.types.DatabaseName
import za.co.absa.ultet.types.function.{FunctionArgumentType, FunctionName}
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.user.UserName


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

    PgFunctionFileParser.parseSource(functionString).head shouldBe FunctionFromSource(
      fnName = FunctionName("public_functionX"),
      paramTypes = Seq(FunctionArgumentType("TEXT"), FunctionArgumentType("INTEGER"), FunctionArgumentType("hstore")),
      owner = UserName("owner_user123"),
      users = Set(UserName("user_for_accessA"), UserName("user_for_accessB"), UserName("user_for_accessZ")),
      schema = SchemaName("my_schema1"),
      database = DatabaseName("eXample_db"),
      sqlBody = functionString // the whole thing
    )

  }

  it should "parse function from another content (empty users, no IN params)" in {
    val functionString =
      """/* comment here and there **/
        |-- owner:
        |     owner_user123
        |-- database: eXample_db (   )
        |CREATE or REPLACE FUNCTION my_schema1.public_functionX(
        |    OUT status              INTEGER,
        |    OUT status_text         TEXT
        |) RETURNS record AS
        |$$
        |-------------------------------------------------------------------------------
        |--
        |-- Function: my_schema.public_function([Function_Param_Count])
        |--      [Descrip""".stripMargin

    PgFunctionFileParser.parseSource(functionString).head shouldBe FunctionFromSource(
      fnName = FunctionName("public_functionX"),
      paramTypes = Seq.empty,
      owner = UserName("owner_user123"),
      users = Set.empty,
      schema = SchemaName("my_schema1"),
      database = DatabaseName("eXample_db"),
      sqlBody = functionString // the whole thing
    )

  }

  it should "parse example function example file" in {
   val testFileUri = getClass.getClassLoader.getResource("public_function_example.sql").toURI

    val parsedDBItemFromSource = PgFunctionFileParser.parseFile(testFileUri).head

    parsedDBItemFromSource.fnName shouldBe FunctionName("public_function")
    parsedDBItemFromSource.schema shouldBe SchemaName("my_schema")
    parsedDBItemFromSource.paramTypes shouldBe Seq(FunctionArgumentType("TEXT"))
    parsedDBItemFromSource.owner shouldBe UserName("some_owner_user")
    parsedDBItemFromSource.users shouldBe Set(UserName("user_for_access"))
    parsedDBItemFromSource.database shouldBe DatabaseName("example_db")
    parsedDBItemFromSource.sqlBody should include("CREATE OR REPLACE FUNCTION my_schema.public_function")
  }
}
