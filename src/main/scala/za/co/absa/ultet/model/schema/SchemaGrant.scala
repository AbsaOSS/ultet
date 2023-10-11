package za.co.absa.ultet.model.schema

import za.co.absa.ultet.model.{SQLEntry, TransactionGroup}
import za.co.absa.ultet.model.TransactionGroup.TransactionGroup

case class SchemaGrant(name: String, roles: Seq[String]) extends SQLEntry {
  override def sqlExpression: String = s"GRANT USAGE ON SCHEMA $name TO ${roles.mkString(", ")};"

  override def transactionGroup: TransactionGroup = TransactionGroup.Objects

  override def orderInTransaction: Int = 70
}
