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

import com.typesafe.scalalogging.Logger
import za.co.absa.ultet.dbitems.DBSchema.{DO_NOT_CHOWN, DO_NOT_TOUCH, logger}
import za.co.absa.ultet.model.{SQLEntry, SchemaName, UserName}
import za.co.absa.ultet.model.schema.{SchemaCreate, SchemaGrant, SchemaOwner}

case class DBSchema(name: SchemaName,
                    ownerName: UserName,
                    users: Seq[UserName]) extends DBItem {
  override def sqlEntries: Seq[SQLEntry] = {
    if (DO_NOT_TOUCH.contains(name.value)) {
      throw new Exception(s"Schema ${name.value} is not allow to be referenced")
    }

    if (DO_NOT_CHOWN.contains(name.value)){
      logger.warn(s"Schema ${name.value} is not allowed to change owner")
      Seq(
        SchemaCreate(name),
        SchemaGrant(name, users)
      )
    } else {
      Seq(
        SchemaCreate(name),
        SchemaOwner(name, ownerName),
        SchemaGrant(name, users)
      )
    }
  }
}

object DBSchema{
  private val logger = Logger(getClass.getName)

  val DO_NOT_TOUCH: Seq[String] = Seq("pg_toast", "pg_catalog", "information_schema")
  val DO_NOT_CHOWN: Seq[String] = Seq("public")
}
