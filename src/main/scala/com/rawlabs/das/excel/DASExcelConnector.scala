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

import java.io.File

import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

import com.typesafe.scalalogging.StrictLogging

/**
 * Manages the lifetime of the Excel file handle. Currently we open once and do not close explicitly.
 */
class DASExcelConnector(excelOptions: DASExcelOptions) extends StrictLogging {

  val filename: String = excelOptions.filename

  // If large, consider streaming or re-open on each query. This is a simple approach:
  private lazy val workbook: Workbook = {
    logger.info(s"Opening Excel file: $filename")
    WorkbookFactory.create(new File(filename))
  }

  def getWorkbook: Workbook = workbook
}
