/*
 * Copyright 2024 RAW Labs S.A.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

package com.rawlabs.das.excel

import com.rawlabs.das.sdk.scala.{DASFunction, DASSdk, DASTable}
import com.rawlabs.protocol.das.v1.functions.FunctionDefinition
import com.rawlabs.protocol.das.v1.tables.TableDefinition
import com.typesafe.scalalogging.StrictLogging

class DASExcel(options: Map[String, String]) extends DASSdk with StrictLogging {

  override def tableDefinitions: Seq[TableDefinition] = Seq.empty

  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  override def getTable(name: String): Option[DASTable] = None

  override def getFunction(name: String): Option[DASFunction] = None

}
