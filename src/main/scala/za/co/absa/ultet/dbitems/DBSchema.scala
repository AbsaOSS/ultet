package za.co.absa.ultet.dbitems

import za.co.absa.ultet.model.SQLEntry
import za.co.absa.ultet.model.schema.{SchemaCreate, SchemaGrant, SchemaOwner}

case class DBSchema(name: String,
                    ownerName: String,
                    users: Seq[String]) extends DBItem {
  override def sqlEntries: Seq[SQLEntry] = {
    Seq(
      SchemaCreate(name),
      SchemaOwner(name, ownerName),
      SchemaGrant(name, users)
    )
  }
}
