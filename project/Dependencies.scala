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

import sbt._

object Dependencies {

  object Versions {
    val scalactic = "3.2.17"
    val scopt = "4.1.0"
    val config = "1.4.2"
    val logback = "1.4.7"
    val scalaLogging = "3.9.5"
    val postgresql = "42.6.0"
    val circe = "0.14.2"
    val balta = "0.1.0"
    val scalatest = "3.2.17"
  }

  lazy val coreDependencies: Seq[ModuleID] = Seq(
    "org.scalactic"               %%  "scalactic"             % Versions.scalactic,
    "com.github.scopt"            %%  "scopt"                 % Versions.scopt,
    "com.typesafe"                %   "config"                % Versions.config,
    "ch.qos.logback"              %   "logback-classic"       % Versions.logback,
    "com.typesafe.scala-logging"  %%  "scala-logging"         % Versions.scalaLogging,
    "org.postgresql"              %   "postgresql"            % Versions.postgresql,
    "io.circe"                    %%  "circe-yaml"            % Versions.circe,
    "io.circe"                    %%  "circe-core"            % Versions.circe,
    "io.circe"                    %%  "circe-generic"         % Versions.circe,
    "io.circe"                    %%  "circe-parser"          % Versions.circe,
    "io.circe"                    %%  "circe-generic-extras"  % Versions.circe,
    "za.co.absa"                  %%  "balta"                 % Versions.balta,

    "org.scalatest"               %%  "scalatest"             % Versions.scalatest  % "test",
  )
}
