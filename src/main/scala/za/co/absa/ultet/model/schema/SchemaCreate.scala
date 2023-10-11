package za.co.absa.ultet.model.schema

import za.co.absa.ultet.model.{SQLEntry, TransactionGroup}
import za.co.absa.ultet.model.TransactionGroup.TransactionGroup

case class SchemaCreate(name: String) extends SQLEntry {
  override def sqlExpression: String = s"CREATE SCHEMA $name;"

  override def transactionGroup: TransactionGroup = TransactionGroup.Objects

  override def orderInTransaction: Int = 55
}
