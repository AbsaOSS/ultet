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

package za.co.absa.ultet.implicits

import za.co.absa.ultet.dbitems.DBItem
import za.co.absa.ultet.util.SqlEntriesPerTransaction

object SetImplicits {

  implicit class DBItemSetEnhancement(val dbItems: Set[DBItem]) extends AnyVal {
    def toSortedGroupedSqlEntries: SqlEntriesPerTransaction = {
      val sqlEntries = dbItems.toSeq.flatMap(_.sqlEntries)
      sqlEntries
        .groupBy(_.transactionGroup)
        .mapValues(_.sortBy(_.orderInTransaction))
    }  }

}
