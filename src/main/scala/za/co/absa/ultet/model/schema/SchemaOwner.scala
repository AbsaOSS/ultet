package za.co.absa.ultet.model.schema

import za.co.absa.ultet.model.{SQLEntry, TransactionGroup}
import za.co.absa.ultet.model.TransactionGroup.TransactionGroup

case class SchemaOwner(name: String, owner: String) extends SQLEntry {
  override def sqlExpression: String = s"ALTER SCHEMA $name OWNER TO $owner;"

  override def transactionGroup: TransactionGroup = TransactionGroup.Objects

  override def orderInTransaction: Int = 60
}

