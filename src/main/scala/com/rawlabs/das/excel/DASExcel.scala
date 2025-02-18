/*
 * Copyright 2025 RAW Labs S.A.
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

/**
 * A high-level manager for Excel data. On initialization, we parse the user-provided config (filename, table
 * definitions, etc.) and create the relevant DASExcelTable objects.
 */
class DASExcel(options: Map[String, String]) extends DASSdk with StrictLogging {

  // Parse user config
  private val excelOptions = new DASExcelOptions(options)

  // Create connector to open workbook, etc.
  private val connector = new DASExcelConnector(excelOptions)

  // Build each table
  private val allTables: Seq[DASExcelTable] = excelOptions.tableConfigs.map { tc =>
    new DASExcelTable(connector, tc)
  }

  // Pre-compute the table definitions (so that repeated calls to tableDefinitions don’t re-parse).
  private val definitions: Seq[TableDefinition] = allTables.map(_.tableDefinition)

  /** Return the list of all known table definitions for this DAS. */
  override def tableDefinitions: Seq[TableDefinition] = definitions

  /** We have no function definitions here, so empty. */
  override def functionDefinitions: Seq[FunctionDefinition] = Seq.empty

  /**
   * Lookup by name. Return the matching table if any.
   */
  override def getTable(name: String): Option[DASTable] = {
    allTables.find(_.tableConfig.name == name)
  }

  /**
   * This DAS doesn’t provide any custom functions, so always None.
   */
  override def getFunction(name: String): Option[DASFunction] = None

}
