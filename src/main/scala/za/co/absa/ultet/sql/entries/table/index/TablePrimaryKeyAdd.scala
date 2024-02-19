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

package za.co.absa.ultet.sql.entries.table.index

import za.co.absa.ultet.model.table.index.TableIndex.PrimaryKey
import za.co.absa.ultet.sql.entries.table.TableAlteration
import za.co.absa.ultet.types.table.TableIdentifier

case class TablePrimaryKeyAdd(tableIdentifier: TableIdentifier, primaryKey: PrimaryKey) extends TableAlteration {
  override def sqlExpression: String = {
    val commandColumnNames = primaryKey.columns.map(_.expression).mkString(", ")

    s"""ALTER TABLE ${tableIdentifier.fullName}
       |ADD CONSTRAINT ${primaryKey.indexName}
       |PRIMARY KEY ($commandColumnNames);""".stripMargin
  }

  override def orderInTransaction: Int = 271
}
