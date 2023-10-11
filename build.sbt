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

import Dependencies._

ThisBuild / organization := "za.co.absa.ultet"

lazy val scala212 = "2.12.17"

ThisBuild / scalaVersion := scala212

lazy val ultet = (project in file("."))
  .settings(
    name := "ultet",
    libraryDependencies ++= coreDependencies,
    publish / skip := true,
    mainClass in assembly := Some("za.co.absa.ultet.Ultet"),
  )
