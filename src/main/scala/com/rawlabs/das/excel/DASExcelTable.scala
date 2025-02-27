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

import scala.jdk.CollectionConverters._

import org.apache.poi.ss.usermodel.{Row, Workbook}
import org.apache.poi.ss.util.{AreaReference, CellRangeAddress}

import com.rawlabs.das.sdk.scala.DASTable
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkInvalidArgumentException, DASSdkUnsupportedException}
import com.rawlabs.protocol.das.v1.query.{PathKey, Qual, SortKey}
import com.rawlabs.protocol.das.v1.tables.{
  Column => ProtoColumn,
  ColumnDefinition,
  Row => ProtoRow,
  TableDefinition,
  TableId
}
import com.rawlabs.protocol.das.v1.types._
import com.typesafe.scalalogging.StrictLogging

/**
 * Represents a single Excel “table”, which is defined by a region (sheet + cell range) plus optional multi-line
 * headers.
 */
class DASExcelTable(connector: DASExcelConnector, val tableConfig: ExcelTableConfig)
    extends DASTable
    with StrictLogging {

  // Build a TableDefinition once, for performance.
  val tableDefinition: TableDefinition = buildTableDefinition()

  /**
   * We do not support writes, so do not define a unique key column. For the same reason, we do not push down path keys
   * or sorting except trivially returning the input.
   */
  override def getTablePathKeys: Seq[PathKey] = Seq.empty

  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = sortKeys

  /**
   * Basic table size estimate: row count from the region, ~10 bytes per column as a heuristic.
   */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    val regionData = readRegionData()
    DASTable.TableEstimate(
      expectedNumberOfRows = regionData.dataRows.size,
      avgRowWidthBytes = regionData.dataRows.size * regionData.columnNames.size * 10)
  }

  /**
   * Provide a textual explanation of how the table is scanned.
   */
  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] = Seq(
    s"Excel file: ${connector.filename}",
    s"Sheet: ${tableConfig.sheet}",
    s"Range: ${tableConfig.region}",
    s"Header rows: ${tableConfig.headerRows}",
    s"Header joiner: '${tableConfig.headerJoiner}'")

  /**
   * Execute the read from the region, returning all data rows.
   */
  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    val regionData = readRegionData()
    // If 'columns' is empty => all columns. Otherwise only the named columns.
    val wantedCols = if (columns.isEmpty) regionData.columnNames else columns

    val rowIt = regionData.dataRows.iterator

    new DASExecuteResult {
      override def hasNext: Boolean = rowIt.hasNext

      override def next(): ProtoRow = {
        val rowValues = rowIt.next()
        val rowBuilder = ProtoRow.newBuilder()

        // For each column, if it is in the wanted list, add it to the row
        regionData.columnNames.zipWithIndex.foreach { case (colName, idx) =>
          if (wantedCols.contains(colName)) {
            val rawValue = rowValues(idx)
            val cellVal = Value.newBuilder().setString(ValueString.newBuilder().setV(rawValue))

            rowBuilder.addColumns(
              ProtoColumn
                .newBuilder()
                .setName(colName)
                .setData(cellVal))
          }
        }
        rowBuilder.build()
      }

      override def close(): Unit = {
        // no-op
      }
    }
  }

  // These operations are not supported for a read-only Excel-based DAS.
  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkUnsupportedException()

  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkUnsupportedException()

  override def delete(rowId: Value): Unit =
    throw new DASSdkUnsupportedException()

  // ---------------------------------------------------------------------------
  // Private Implementation
  // ---------------------------------------------------------------------------

  /**
   * Build the table definition, including columns derived from the region’s header (or auto-generated).
   */
  private def buildTableDefinition(): TableDefinition = {
    val data = readRegionData()

    val defBuilder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableConfig.name))
      .setDescription(s"Excel sheet=${tableConfig.sheet} region=${tableConfig.region}")

    // Each column is defined as a STRING (nullable) for simplicity.
    data.columnNames.foreach { colName =>
      val colDef = ColumnDefinition
        .newBuilder()
        .setName(colName)
        .setType(Type.newBuilder().setString(StringType.newBuilder().setNullable(true)))
      defBuilder.addColumns(colDef)
    }

    defBuilder.build()
  }

  /**
   * Reads all data from the specified region, returning column names (from headers or auto-named) and the actual data
   * rows.
   */
  private def readRegionData(): ExcelRegionData = {
    val wb: Workbook = connector.getWorkbook
    val sheet = wb.getSheet(tableConfig.sheet)
    if (sheet == null) {
      throw new DASSdkInvalidArgumentException(
        s"sheet '${tableConfig.sheet}' not found in workbook '${connector.filename}'")
    }

    // Parse range string, e.g. "A1:D50"
    val areaRef = new AreaReference(tableConfig.region, wb.getSpreadsheetVersion)
    val firstRowIdx = areaRef.getFirstCell.getRow
    val lastRowIdx = areaRef.getLastCell.getRow
    val firstColIdx = areaRef.getFirstCell.getCol
    val lastColIdx = areaRef.getLastCell.getCol

    // Gather merged regions in this sheet so we can replicate top-left values properly.
    val merges = sheet.getMergedRegions.asScala.toList

    // Build a raw matrix: rows × columns, each cell as string
    val rawMatrix: Seq[Seq[String]] = (firstRowIdx to lastRowIdx).map { rIdx =>
      val rowObj = sheet.getRow(rIdx)
      (firstColIdx to lastColIdx).map { cIdx =>
        readCellValue(rowObj, cIdx, merges)
      }
    }

    if (rawMatrix.isEmpty) {
      // No rows, no columns
      return ExcelRegionData(Nil, Nil)
    }

    // According to tableConfig, how many top rows are headers?
    val hdrRows = tableConfig.headerRows
    if (hdrRows == 0) {
      // No headers => auto-name columns A,B,C,...
      val colCount = rawMatrix.head.size
      val colNames = (0 until colCount).map(excelColumnName).toList
      val dataRows = rawMatrix.map(_.toList).toList
      return ExcelRegionData(colNames, dataRows)
    }

    // If we do have header rows, combine them to form a single header line per column.
    val actualHdrRows = math.min(hdrRows, rawMatrix.size)
    val headerPart = rawMatrix.take(actualHdrRows)
    val dataPart = rawMatrix.drop(actualHdrRows).map(_.toList).toList

    // We'll combine each column’s header cell across all hdrRows using the "headerJoiner".
    val colCount = headerPart.head.size
    val joinedHeaders = (0 until colCount).map { cIdx =>
      val lines = headerPart.map(row => row(cIdx).trim).filter(_.nonEmpty)
      if (lines.isEmpty) "" else lines.mkString(tableConfig.headerJoiner)
    }.toList

    // Convert the joined headers to final column names (handle empty, duplicates, truncation).
    val finalNames = finalizeColumnNames(joinedHeaders)
    ExcelRegionData(finalNames, dataPart)
  }

  /**
   * If (rowIdx, colIdx) is within a merged region, replicate the top-left cell value. Otherwise just read the cell in
   * this row directly.
   */
  private def readCellValue(rowObj: Row, cIdx: Int, merges: List[CellRangeAddress]): String = {
    if (rowObj == null) {
      // Missing row => empty string
      return ""
    }
    val rowIndex = rowObj.getRowNum
    val (topRow, leftCol) = findTopLeftOfMerged(rowIndex, cIdx, merges)

    val realRowObj = rowObj.getSheet.getRow(topRow)
    if (realRowObj == null) {
      return ""
    }
    val realCell = realRowObj.getCell(leftCol, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
    if (realCell == null) {
      return ""
    }
    realCell.toString
  }

  /**
   * If the given (rowIdx, colIdx) belongs to a merged region, return the top-left corner of that region. Otherwise,
   * return (rowIdx, colIdx) as is.
   */
  private def findTopLeftOfMerged(rowIdx: Int, colIdx: Int, merges: List[CellRangeAddress]): (Int, Int) = {
    merges.find { mr =>
      rowIdx >= mr.getFirstRow && rowIdx <= mr.getLastRow &&
      colIdx >= mr.getFirstColumn && colIdx <= mr.getLastColumn
    } match {
      case Some(region) =>
        (region.getFirstRow, region.getFirstColumn)
      case None =>
        (rowIdx, colIdx)
    }
  }

  /**
   * Convert a 0-based column index to Excel-style letters: 0 => "A", 1 => "B", 25 => "Z", 26 => "AA", etc. Used for
   * auto-naming columns when headerRows=0.
   */
  private def excelColumnName(colIndex: Int): String = {
    var dividend = colIndex + 1
    val sb = new StringBuilder
    while (dividend > 0) {
      val modulo = (dividend - 1) % 26
      sb.insert(0, (65 + modulo).toChar) // 'A'=65
      dividend = (dividend - modulo - 1) / 26
    }
    sb.toString()
  }

  /**
   * Finalize column names: 1) Empty => "column_1", "column_2", ... 2) Truncate to 63 bytes (common SQL identifier
   * length). 3) Deduplicate with `_2, _3, ...` as needed.
   */
  private def finalizeColumnNames(rawNames: List[String]): List[String] = {
    val used = scala.collection.mutable.HashMap.empty[String, Int]
    var anonymousCount = 0

    rawNames.map { name =>
      val n0 = if (name.trim.isEmpty) {
        anonymousCount += 1
        s"column_$anonymousCount"
      } else {
        name
      }

      // Truncate to 63 bytes
      val truncated = truncateToBytes(n0, 63)

      // Deduplicate if necessary
      if (!used.contains(truncated)) {
        used(truncated) = 1
        truncated
      } else {
        val cnt = used(truncated)
        used(truncated) = cnt + 1

        val candidate = s"${truncated}_$cnt"
        val finalName = truncateToBytes(candidate, 63)

        // If finalName is different from truncated, we might also need to track it
        used.get(finalName) match {
          case None    => used(finalName) = 1
          case Some(x) => used(finalName) = x + 1
        }
        finalName
      }
    }
  }

  /**
   * Truncates the given string `s` to at most `maxBytes` in UTF‐8 encoding. Ensures we don't cut in the middle of a
   * multi‐byte character.
   */
  private def truncateToBytes(s: String, maxBytes: Int): String = {
    val bytes = s.getBytes("UTF-8")
    if (bytes.length <= maxBytes) s
    else {
      new String(bytes.take(maxBytes), "UTF-8")
    }
  }

  /**
   * Simple container for final data: column names + list of data rows.
   */
  private case class ExcelRegionData(columnNames: List[String], dataRows: List[List[String]])
}
