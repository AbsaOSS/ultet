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

package za.co.absa.ultet.sql.entries.user

import za.co.absa.ultet.sql.TransactionGroup
import za.co.absa.ultet.sql.TransactionGroup.TransactionGroup
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.types.user.UserName

case class UserCreation(name: UserName) extends SQLEntry {
  override def sqlExpression: String = s"""DO
                                          |$$do$$
                                          |  BEGIN
                                          |    IF EXISTS (
                                          |      SELECT FROM pg_catalog.pg_roles
                                          |      WHERE lowercase(rolname) = '${name.normalized}') THEN
                                          |
                                          |      RAISE NOTICE 'Role "${name.value}" already exists. Skipping.';
                                          |    ELSE
                                          |      CREATE ROLE ${name.normalized};
                                          |    END IF;
                                          |  END
                                          |$$do$$;""".stripMargin

  override def transactionGroup: TransactionGroup = TransactionGroup.Roles

  override def orderInTransaction: Int = 10
}
