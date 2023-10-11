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

package za.co.absa.ultet.model

package object function {

  case class FunctionName(value: String) extends AnyVal

  case class ArgumentName(value: String) extends AnyVal
  case class ArgumentType(value: String) extends AnyVal
  type FunctionArguments = Seq[(ArgumentName, ArgumentType)]

  case class OutputType(value: String) extends AnyVal
  case class OutputRecordKey(value: String) extends AnyVal

  sealed trait FunctionOutput
  case object NoOutput extends FunctionOutput
  case class SingleValueOutput(outputType: OutputType) extends FunctionOutput
  case class RecordOutput(record: Seq[(OutputRecordKey, OutputType)]) extends FunctionOutput

  case class FunctionBodyCode(value: String) extends AnyVal

}
