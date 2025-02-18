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

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import com.rawlabs.protocol.das.v1.tables.{Row => ProtoRow}

class DASExcelTest extends AnyFunSuite {

  // --------------------------------------------------------------------------
  // Configuration for the test
  // --------------------------------------------------------------------------

  // Typically, you'd place "Book1.xlsx" under "src/test/resources/Book1.xlsx".
  // Then you can get an absolute path or resource. For simplicity, we assume an absolute path:
  private val excelFilePath = getClass.getResource("/Book1.xlsx").getPath

  // The options that replicate your environment:
  private val options: Map[String, String] = Map(
    "filename" -> excelFilePath,
    "nr_tables" -> "4",
    // TableA
    "table0_name" -> "TableA",
    "table0_sheet" -> "Sheet1",
    "table0_region" -> "A1:C4",
    "table0_headers" -> "true",
    // TableB
    "table1_name" -> "TableB",
    "table1_sheet" -> "Sheet1",
    "table1_region" -> "E8:F9",
    "table1_headers" -> "false",
    // TableC
    "table2_name" -> "TableC",
    "table2_sheet" -> "Sheet2",
    "table2_region" -> "B4:C6",
    "table2_headers" -> "true",
    // TableD
    "table3_name" -> "TableD",
    "table3_sheet" -> "Sheet1",
    "table3_region" -> "C14:D20",
    "table3_headers" -> "true")

  // Create the DAS instance from these options
  private val dasExcel = new DASExcel(options)

  // Grab the tables from the DAS
  private val tableA: Option[DASExcelTable] = dasExcel.getTable("TableA")
  private val tableB: Option[DASExcelTable] = dasExcel.getTable("TableB")
  private val tableC: Option[DASExcelTable] = dasExcel.getTable("TableC")
  private val tableD: Option[DASExcelTable] = dasExcel.getTable("TableD")

  // --------------------------------------------------------------------------
  // 1) Basic presence and definitions
  // --------------------------------------------------------------------------

  test("There should be exactly 4 table definitions returned") {
    val defs = dasExcel.tableDefinitions
    assert(defs.size == 4, s"Expected 4 definitions, got ${defs.size}")
  }

  test("TableA definition should exist with expected columns") {
    assert(tableA.isDefined, "TableA must be defined")
    val tableADef = tableA.get.tableDefinition
    val colNames = tableADef.getColumnsList
    val actualNames = colNames.asScala.map(_.getName)
    assert(actualNames == Seq("ColA", "ColB", "Column C"), s"Expected columns, got $actualNames")
  }

  test("TableB definition should exist with expected columns") {
    assert(tableB.isDefined, "TableB must be defined")
    val tableBDef = tableB.get.tableDefinition
    val colNames = tableBDef.getColumnsList
    val actualNames = colNames.asScala.map(_.getName)
    assert(actualNames == Seq("A", "B"), s"Expected columns, got $actualNames")
  }

  test("TableC definition should exist with expected columns") {
    assert(tableC.isDefined, "TableC must be defined")
    val tableCDef = tableC.get.tableDefinition
    val colNames = tableCDef.getColumnsList
    val actualNames = colNames.asScala.map(_.getName)
    assert(actualNames == Seq("Name", "Age"), s"Expected columns, got $actualNames")
  }

  test("TableD definition should exist with expected columns") {
    assert(tableD.isDefined, "TableD must be defined")
    val tableDDef = tableD.get.tableDefinition
    val colNames = tableDDef.getColumnsList
    val actualNames = colNames.asScala.map(_.getName)
    assert(actualNames == Seq("Column 1", "Column 2"), s"Expected columns, got $actualNames")
  }

  // --------------------------------------------------------------------------
  // 2) Execution / data checks
  // --------------------------------------------------------------------------

  test("TableA should have the correct rows (headers=true)") {
    val dt = tableA.get
    val execResult = dt.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)

    val rowsBuffer = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (execResult.hasNext) {
      rowsBuffer += execResult.next()
    }
    execResult.close()

    assert(rowsBuffer.size == 3)
    assert(rowsBuffer(0).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("A", "1.0", "One"))
    assert(rowsBuffer(1).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("B", "2.0", "Two"))
    assert(rowsBuffer(2).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("C", "3.0", "Three"))
  }

  test("TableB (headers=false) should have correct data") {
    val dt = tableB.get
    val execResult = dt.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)
    val rowsBuffer = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (execResult.hasNext) {
      rowsBuffer += execResult.next()
    }
    execResult.close()

    assert(rowsBuffer.size == 2)
    assert(rowsBuffer(0).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("X", "XX"))
    assert(rowsBuffer(1).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("Y", "YY"))
  }

  test("TableC (headers=true) data test") {
    val dt = tableC.get
    val execResult = dt.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)
    val rowsBuffer = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (execResult.hasNext) {
      rowsBuffer += execResult.next()
    }
    execResult.close()

    assert(rowsBuffer.size == 2)
    assert(rowsBuffer(0).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("Ben", "99.0"))
    assert(rowsBuffer(1).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("Miguel", "19.0"))
  }

  test("TableD (headers=true) data test") {
    val dt = tableD.get
    val execResult = dt.execute(quals = Seq.empty, columns = Seq.empty, sortKeys = Seq.empty, maybeLimit = None)
    val rowsBuffer = scala.collection.mutable.ArrayBuffer.empty[ProtoRow]
    while (execResult.hasNext) {
      rowsBuffer += execResult.next()
    }
    execResult.close()

    assert(rowsBuffer.size == 6)
    assert(rowsBuffer(0).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("1.0", "Foo"))
    assert(rowsBuffer(1).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("2.0", "Foo"))
    assert(rowsBuffer(2).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("3.0", "Foo"))
    assert(rowsBuffer(3).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("4.0", "Bar"))
    assert(rowsBuffer(4).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("5.0", "Bar"))
    assert(rowsBuffer(5).getColumnsList.asScala.map(_.getData.getString.getV) == Seq("6.0", "Cucu"))
  }

}
