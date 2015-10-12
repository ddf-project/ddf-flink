package io.ddf.flink.analytics

import java.util

import io.ddf.DDF
import io.ddf.content.Schema.{Column, ColumnClass}
import io.ddf.flink.BaseSpec

import scala.collection.JavaConversions._

class BinningHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()

  val monthColumnLabel: String = "Month"

  it should "bin by equal interval" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "EQUALINTERVAL", 2, null, true, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"[1,6]" = 26,"(6,11]" = 5}
    monthColumn.getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("[1,6]") should be(26)
    levelCounts.get("(6,11]") should be(5)
    levelCounts.values().reduce(_ + _) should be(31)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (31)
  }

  it should "bin by equal frequency" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "EQUAlFREQ", 2, null, true, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  [1,1] -> 17--(1,11] -> 14
    monthColumn.getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("[1,1]") should be(17)
    levelCounts.values().reduce(_ + _) should be(31)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (31)
  }

  it should "bin by custom interval" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "custom", 0, Array[Double](2, 4, 6, 8), true, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn("Month")
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    // {"[2,4]"=6, "(4,6]"=3, "(6,8]"=2}
    monthColumn.getOptionalFactor.getLevelMap.size should be(3)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("[2,4]") should be(6)
    levelCounts.get("(4,6]") should be(3)
    levelCounts.get("(6,8]") should be(2)
    levelCounts.values().reduce(_ + _) should be(11)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (11)
  }

  it should "bin by equal interval excluding highest" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "EQUALINTERVAL", 2, null, true, false)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"[1,6)" = 24,"[6,11)" = 6}
    monthColumn.getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("[1,6)") should be(24)
    levelCounts.get("[6,11)") should be(6)
    levelCounts.values().reduce(_ + _) should be(30)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (30)
  }

  it should "bin by equal interval excluding lowest" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "EQUALINTERVAL", 2, null, false, true)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"(1,6]" = 9,"(6,11]" = 5}
    monthColumn.getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("(1,6]") should be(9)
    levelCounts.get("(6,11]") should be(5)
    levelCounts.values().reduce(_ + _) should be(14)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (14)
  }

  it should "bin by equal interval excluding lowest and highest" in {
    val newDDF: DDF = airlineDDF.binning(monthColumnLabel, "EQUALINTERVAL", 2, null, false, false)

    val monthColumn: Column = newDDF.getSchemaHandler.getColumn(monthColumnLabel)
    monthColumn.getColumnClass should be(ColumnClass.FACTOR)
    //  {"(1,6)" = 7,"(6,11)" = 4}
    monthColumn.getOptionalFactor.getLevelMap.size should be(2)
    val levelCounts: util.Map[String, Integer] = monthColumn.getOptionalFactor.getLevelCounts
    levelCounts.get("(1,6)") should be(7)
    levelCounts.get("(6,11)") should be(4)
    levelCounts.values().reduce(_ + _) should be(11)
    newDDF.sql(s"select $monthColumnLabel from @this", "").getRows should have size (11)
  }
}
