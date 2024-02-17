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

package za.co.absa.ultet.dbitems

import za.co.absa.ultet.model.{DatabaseName, SQLEntry, SchemaName, UserName}
import za.co.absa.ultet.model.function.{FunctionArgumentType, FunctionBody, FunctionGrant, FunctionName, FunctionOwnership}

case class DBFunctionFromSource(
                                 fnName: FunctionName,
                                 paramTypes: Seq[FunctionArgumentType],
                                 owner: UserName,
                                 users: Set[UserName],
                                 schema: SchemaName,
                                 database: DatabaseName,
                                 sqlBody: String
                               ) extends DBFunction {
  override def sqlEntries: Seq[SQLEntry] = {
    Seq(FunctionBody(sqlBody)) ++
    users.map { user =>
      // e.g. GRANT EXECUTE ON FUNCTION schema.fnName(UUID, TEXT, INTEGER, JSONB, JSONB) TO user;
      FunctionGrant(schema, fnName, paramTypes, user)
    } ++
    Seq(FunctionOwnership(schema, fnName, paramTypes, owner))
  }

}
