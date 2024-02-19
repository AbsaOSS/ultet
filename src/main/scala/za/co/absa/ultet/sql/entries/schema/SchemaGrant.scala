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

package za.co.absa.ultet.sql.entries.schema

import za.co.absa.ultet.sql.TransactionGroup
import za.co.absa.ultet.sql.TransactionGroup.TransactionGroup
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.types.schema.SchemaName
import za.co.absa.ultet.types.user.UserName

case class SchemaGrant(name: SchemaName, roles: Set[UserName]) extends SQLEntry {
  override def sqlExpression: String = {
    s"GRANT USAGE ON SCHEMA ${name.normalized} TO ${roles.map(_.normalized).mkString(", ")};"
  }

  override def transactionGroup: TransactionGroup = TransactionGroup.Objects

  override def orderInTransaction: Int = 70
}
