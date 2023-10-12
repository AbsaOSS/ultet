/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.ultet.util

import com.typesafe.config.ConfigFactory

import java.io.File
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

// Example of the connection string
// jdbc:postgresql://[serverName[:portNumber]]/[database][?property=value[;property=value]]
//
// Example of the properties file
// serverName = "myServer"
// database = "myDatabase"
// portNumber = "5432"
// protocol = "jdbc"
// subprotocol = "postgresql"
// properties {
//   user = "admin"
//   password = "password"
// }

case class DBProperties(serverName: String,
                        database: String,
                        portNumber: Option[String],
                        properties: Map[String, String] = Map.empty,
                        user: String,
                        password: String,
                        protocol: String = "jdbc",
                        subprotocol: String = "postgresql") {
  def generateConnectionString(): String = {
    subprotocol match {
      case "postgresql" => getPostgresString
      case _ => throw new NotImplementedError(s"Subprotocol $subprotocol is not implemented yet")
    }
  }

  private def getPostgresString: String = {
    val port = portNumber.map(p => s":$p").getOrElse("")
    val props = if (properties.isEmpty) {
      ""
    }
    else {
      "?" + properties.map { case (key, value) => s"$key=$value" }.mkString(";")
    }

    s"$protocol:$subprotocol://$serverName$port/$database$props"
  }
}

object DBProperties {
  def getSysDB(dbProperties: DBProperties): DBProperties = {
    dbProperties.subprotocol match {
      case "postgresql" => dbProperties.copy(database = "postgres")
      case _ => throw new NotImplementedError(s"Subprotocol ${dbProperties.subprotocol} is not implemented yet")
    }
  }

  def loadProperties(filePath: String): DBProperties = {
    val config = ConfigFactory.parseFile(new File(filePath))

    // Extract required properties
    val serverName = config.getString("serverName")
    val database = config.getString("database")
    val user = config.getString("user")
    val password = config.getString("password")
    val portNumber = if(config.hasPath("portNumber")) Some(config.getString("portNumber")) else None
    val protocol = if(config.hasPath("protocol")) Some(config.getString("protocol")) else None
    val subprotocol = if(config.hasPath("subprotocol")) Some(config.getString("subprotocol")) else None

    // Assume other properties are additional properties for the connection.
    val propertiesConfig = config.getConfig("properties")
    val properties = propertiesConfig.entrySet().map { entry =>
      entry.getKey -> propertiesConfig.getString(entry.getKey)
    }.toMap

    (protocol, subprotocol) match {
      case (Some(p), Some(s)) => DBProperties(serverName, database, portNumber, properties, p, s)
      case (_, Some(s)) => DBProperties(serverName, database, portNumber, properties, subprotocol = s)
      case (_,_) => DBProperties(serverName, database, portNumber, properties)
    }
  }
}
