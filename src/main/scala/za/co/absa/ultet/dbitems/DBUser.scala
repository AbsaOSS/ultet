package za.co.absa.ultet.dbitems

import za.co.absa.ultet.model.{SQLEntry, UserEntry}

case class DBUser(name: String) extends DBItem {

  override def sqlEntries: Seq[SQLEntry] = {
    Seq(UserEntry(name))
  }
}
