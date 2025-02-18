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
import com.rawlabs.das.sdk.{DASExecuteResult, DASSdkException}
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
 * Represents a single Excel “table”, which is actually a region in a given sheet.
 */
class DASExcelTable(connector: DASExcelConnector, val tableConfig: ExcelTableConfig)
    extends DASTable
    with StrictLogging {

  // Build definition once
  val tableDefinition: TableDefinition = buildTableDefinition()

  /**
   * We do not support writes. So we do not define a unique column. For the same reason, path keys or sorting pushdown
   * are minimal.
   */
  override def getTablePathKeys: Seq[PathKey] = Seq.empty

  override def getTableSortOrders(sortKeys: Seq[SortKey]): Seq[SortKey] = sortKeys

  /** Basic estimate: row count from the region, assume ~10 bytes/column. */
  override def tableEstimate(quals: Seq[Qual], columns: Seq[String]): DASTable.TableEstimate = {
    val regionData = readRegionData()
    DASTable.TableEstimate(
      expectedNumberOfRows = regionData.dataRows.size,
      avgRowWidthBytes = regionData.dataRows.size * regionData.columnNames.size * 10)
  }

  /** No fancy pushdown, just a textual explanation. */
  override def explain(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): Seq[String] = {
    Seq(
      s"Excel file: ${connector.filename}",
      s"Sheet: ${tableConfig.sheet}",
      s"Range: ${tableConfig.region}",
      s"Headers: ${tableConfig.headers}",
      "Merged cells repeated? yes")
  }

  override def execute(
      quals: Seq[Qual],
      columns: Seq[String],
      sortKeys: Seq[SortKey],
      maybeLimit: Option[Long]): DASExecuteResult = {

    val regionData = readRegionData()
    // final columns to produce (by name)
    val wantedCols = if (columns.isEmpty) regionData.columnNames else columns

    val rowIt = regionData.dataRows.iterator
    new DASExecuteResult {
      override def hasNext: Boolean = rowIt.hasNext

      override def next(): ProtoRow = {
        val rowValues = rowIt.next() // e.g. Seq of string cells
        val rowBuilder = ProtoRow.newBuilder()

        // For each column in regionData.columnNames, if it’s wanted, add a proto column
        // rowValues is aligned with regionData.columnNames
        regionData.columnNames.zipWithIndex.foreach { case (colName, idx) =>
          if (wantedCols.contains(colName)) {
            val rawStr = rowValues(idx)
            val cellValue = Value.newBuilder().setString(ValueString.newBuilder().setV(rawStr))
            rowBuilder.addColumns(ProtoColumn.newBuilder().setName(colName).setData(cellValue))
          }
        }
        rowBuilder.build()
      }

      override def close(): Unit = {
        // no-op
      }
    }
  }

  override def insert(row: ProtoRow): ProtoRow =
    throw new DASSdkException("Excel DAS is read-only: insert not supported.")

  override def update(rowId: Value, newRow: ProtoRow): ProtoRow =
    throw new DASSdkException("Excel DAS is read-only: update not supported.")

  override def delete(rowId: Value): Unit =
    throw new DASSdkException("Excel DAS is read-only: delete not supported.")

  // ---------------------------------------------------------------------------
  // Private / Implementation
  // ---------------------------------------------------------------------------

  private def buildTableDefinition(): TableDefinition = {
    val data = readRegionData()
    val defBuilder = TableDefinition
      .newBuilder()
      .setTableId(TableId.newBuilder().setName(tableConfig.name))
      .setDescription(s"Excel sheet=${tableConfig.sheet} region=${tableConfig.region}")

    // Add columns
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
   * Reads all data from the region, returning column names and row data.
   */
  private def readRegionData(): ExcelRegionData = {
    val wb: Workbook = connector.getWorkbook
    val sheet = wb.getSheet(tableConfig.sheet)
    if (sheet == null) {
      throw new DASSdkException(s"Sheet '${tableConfig.sheet}' not found in workbook '${connector.filename}'")
    }

    // Parse range string, e.g. "A1:D50"
    val areaRef = new AreaReference(tableConfig.region, wb.getSpreadsheetVersion)
    val firstRowIdx = areaRef.getFirstCell.getRow
    val lastRowIdx = areaRef.getLastCell.getRow
    val firstColIdx = areaRef.getFirstCell.getCol
    val lastColIdx = areaRef.getLastCell.getCol

    // Precompute merged region info to replicate top-left content.
    val merges = sheet.getMergedRegions.asScala.toList

    // Collect all rows
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

    // If headers=true, first row is column names
    if (tableConfig.headers && rawMatrix.nonEmpty) {
      val header = rawMatrix.head
      val colNames = header.zipWithIndex.map { case (txt, idx) =>
        val trimmed = txt.trim
        if (trimmed.nonEmpty) trimmed else s"column_${idx + 1}"
      }
      val dataRows = rawMatrix.tail.map(_.toList).toList
      ExcelRegionData(colNames.toList, dataRows)
    } else {
      // If headers=false, we name columns with A,B,C,...
      val numCols = rawMatrix.head.size
      val colNames = (0 until numCols).map(idx => excelColumnName(idx)).toList
      val dataRows = rawMatrix.map(_.toList).toList
      ExcelRegionData(colNames, dataRows)
    }
  }

  /**
   * Returns the actual string for the cell at column cIdx in rowObj, including repeating merged content if inside a
   * merged region.
   */
  private def readCellValue(rowObj: Row, cIdx: Int, merges: List[CellRangeAddress]): String = {
    if (rowObj == null) {
      // Entire row is missing => empty string
      return ""
    }
    // 1) find if cIdx in a merged region
    val (topRow, leftCol) = findTopLeftOfMerged(rowObj.getRowNum, cIdx, merges)

    // That top-left cell is the “real” source of content
    val realRowObj = rowObj.getSheet.getRow(topRow)
    if (realRowObj == null) {
      return ""
    }
    val realCell = realRowObj.getCell(leftCol, Row.MissingCellPolicy.RETURN_NULL_AND_BLANK)
    if (realCell == null) {
      return ""
    }
    // Convert to string
    realCell.toString
  }

  /**
   * If (rowIdx,colIdx) belongs to a merged region, returns the top-left corner of that region. Otherwise returns
   * (rowIdx, colIdx) itself.
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
   * Convert a 0-based column index to Excel’s A, B, C, ... Z, AA, AB, ...
   */
  private def excelColumnName(colIndex: Int): String = {
    // e.g. colIndex=0 => "A", 1 => "B", 25 => "Z", 26 => "AA"
    var dividend = colIndex + 1
    val sb = new StringBuilder
    while (dividend > 0) {
      val modulo = (dividend - 1) % 26
      sb.insert(0, (65 + modulo).toChar) // 65 => 'A'
      dividend = (dividend - modulo - 1) / 26
    }
    sb.toString()
  }

  // Simple container for final data
  private case class ExcelRegionData(columnNames: List[String], dataRows: List[List[String]])
}
