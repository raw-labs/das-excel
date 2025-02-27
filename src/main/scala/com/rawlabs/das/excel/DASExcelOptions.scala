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

import com.rawlabs.das.sdk.DASSdkInvalidArgumentException

/**
 * Represents a single table’s configuration: which sheet, range, and how headers are defined.
 *
 * @param name The table name.
 * @param sheet The sheet name.
 * @param region The cell region, e.g. "A1:C10".
 * @param headerRows How many rows from the top of the region are considered "header rows" (0 means no headers).
 * @param headerJoiner The string used to join multiple header lines into one. Default is "-" if not specified.
 */
case class ExcelTableConfig(name: String, sheet: String, region: String, headerRows: Int, headerJoiner: String)

/**
 * Holds all parsed config from the user's definitions for the entire DAS.
 */
class DASExcelOptions(options: Map[String, String]) {

  // The path to the Excel file is always required.
  val filename: String =
    options.getOrElse("filename", throw new DASSdkInvalidArgumentException("missing 'filename' option"))

  // The number of tables (regions) must also be specified.
  val nrTables: Int = options
    .get("nr_tables")
    .map(_.toInt)
    .getOrElse(throw new DASSdkInvalidArgumentException("missing 'nr_tables' option"))

  /**
   * Build a list of ExcelTableConfig from user’s config keys: table0_name=..., table0_sheet=..., table0_region=...,
   * table0_header_rows=..., table0_header_joiner=...
   */
  val tableConfigs: Seq[ExcelTableConfig] = {
    (0 until nrTables).map { i =>
      val prefix = s"table${i}_"

      val tblName = options.getOrElse(prefix + "name", s"excel_table_$i")
      val sheet = options.getOrElse(prefix + "sheet", "Sheet1")
      val region = options.getOrElse(
        prefix + "region",
        throw new DASSdkInvalidArgumentException(s"missing '${prefix}region' option"))
      // Default 0 means no headers. If user specified e.g. 2, we combine the first 2 rows into the header.
      val hdrRows = options.get(prefix + "header_rows").map(_.toInt).getOrElse(0)
      // Default joiner is "-"
      val hdrJoin = options.getOrElse(prefix + "header_joiner", "-")

      ExcelTableConfig(name = tblName, sheet = sheet, region = region, headerRows = hdrRows, headerJoiner = hdrJoin)
    }
  }

}
