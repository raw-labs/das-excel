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

/**
 * Represents a single table’s configuration: which sheet, range, etc.
 */
case class ExcelTableConfig(name: String, sheet: String, region: String, headers: Boolean)

/**
 * Holds all parsed config from user’s definition for the entire DAS.
 */
class DASExcelOptions(options: Map[String, String]) {

  // Required
  val filename: String =
    options.getOrElse("filename", throw new IllegalArgumentException("Missing 'filename' option for Excel DAS."))

  val nrTables: Int = options.get("nr_tables").map(_.toInt).getOrElse(1)

  /**
   * Build a list of ExcelTableConfig from user’s config keys: e.g. table0_name=..., table0_sheet=...,
   * table0_region=..., table0_headers=true/false
   */
  val tableConfigs: Seq[ExcelTableConfig] = {
    (0 until nrTables).map { i =>
      val prefix = s"table${i}_"
      val tblName = options.getOrElse(prefix + "name", s"excel_table_$i")
      val sheet = options.getOrElse(prefix + "sheet", "Sheet1")
      val region = options.getOrElse(prefix + "region", "A1:D10")
      // "true"/"false" => default false
      val headers = options.get(prefix + "headers").exists(_.trim.equalsIgnoreCase("true"))

      ExcelTableConfig(name = tblName, sheet = sheet, region = region, headers = headers)
    }
  }

}
