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
package za.co.absa.ultet.model.function

import za.co.absa.ultet.model.{SchemaName, UserName}

case class FunctionGrant(
                          schemaName: SchemaName,
                          functionName: FunctionName,
                          arguments: FunctionArguments,
                          userToGrantExecuteTo: UserName
                        ) extends FunctionEntry {
  override def sqlExpression: String = {
    s"""GRANT EXECUTE ON FUNCTION
       |  ${schemaName.value}.${functionName.value}($argumentTypesListAsString)
       |TO
       |  ${userToGrantExecuteTo.value};
       |""".stripMargin
  }

  override def transactionGroup: String = ???

  override def order: Int = 102
}
