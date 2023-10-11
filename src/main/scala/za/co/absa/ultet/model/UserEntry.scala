package za.co.absa.ultet.model

import za.co.absa.ultet.model.TransactionGroup.TransactionGroup

case class UserEntry(name: String) extends SQLEntry {
  override def sqlExpression: String = s"CREATE USER $name;"

  override def transactionGroup: TransactionGroup = TransactionGroup.Roles

  override def orderInTransaction: Int = 10
}
