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

ThisBuild / organizationHomepage := Some(url("https://www.absa.africa"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    browseUrl = url("https://github.com/AbsaOSS/ultet/tree/master"),
    connection = "scm:git:git://github.com/AbsaOSS/ultet.git",
    devConnection = "scm:git:ssh://github.com/AbsaOSS/ultet.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id = "benedeki",
    name = "David Benedeki",
    email = "david.benedeki@absa.africa",
    url = url("https://github.com/benedeki")
  ),
  Developer(
    id    = "dk1844",
    name  = "Daniel Kavan",
    email = "daniel.kavan@absa.africa",
    url   = url("https://github.com/dk1844")
  ),
  Developer(
    id = "jakipatryk",
    name = "Bartlomiej Baj",
    email = "bartlomiej.baj@absa.africa",
    url = url("https://github.com/jakipatryk")
  ),
  Developer(
    id = "lsulak",
    name = "Ladislav Sulak",
    email = "ladislav.sulak@absa.africa",
    url = url("https://github.com/lsulak")
  ),
  Developer(
    id = "Zejnilovic",
    name = "Sasa Zejnilovic",
    email = "sasa.zejnilovic@absa.africa",
    url = url("https://github.com/Zejnilovic")
  )
)

ThisBuild / description := "Database deployment tool"

ThisBuild / organizationName := "ABSA Group Limited"
ThisBuild / startYear := Some(2023)
ThisBuild / licenses += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")