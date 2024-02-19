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

package za.co.absa.ultet.model.table

import za.co.absa.ultet.model.DBItem
import za.co.absa.ultet.sql.entries.SQLEntry
import za.co.absa.ultet.sql.entries.table.index.{TableIndexCreation, TablePrimaryKeyAdd}
import za.co.absa.ultet.sql.entries.table.{TableAlteration, TableCreation, TableDescriptionSet}

case class TableInsert(table: TableDef) extends DBItem {
  override def sqlEntries: Seq[SQLEntry] = {
    val pkCreateAlteration: Option[TableAlteration] = table.primaryKey.map(definedPk => TablePrimaryKeyAdd(table.tableIdentifier, definedPk))
    val indicesCreateAlterations = table.indexes.map(idx => TableIndexCreation(table.tableIdentifier, idx))
    val tableDescription =  table.description.map(d =>TableDescriptionSet(table.tableIdentifier, Some(d)))

      Seq(TableCreation(table.tableIdentifier, table.columns)) ++
      tableDescription ++
      pkCreateAlteration ++
      indicesCreateAlterations
  }
}
