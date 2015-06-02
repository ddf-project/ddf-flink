package io.flink.ddf

import io.ddf.{DDF, DDFManager}
import org.scalatest.{FlatSpec, Matchers}

class BaseSpec extends FlatSpec with Matchers {
  val flinkDDFManager = DDFManager.get("flink").asInstanceOf[FlinkDDFManager]
  val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  def loadAirlineDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = flinkDDFManager.getDDFByName("airline")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql2txt("create table airline (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airline.csv").getPath
        flinkDDFManager.sql2txt("load '" + filePath + "' into airline")
        ddf = flinkDDFManager.getDDFByName("airline")
    }
    ddf
  }

  def loadYearNamesDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = flinkDDFManager.getDDFByName("year_names")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql2txt("create table year_names (Year_num int,Name string)")
        val filePath = getClass.getResource("/year_names.csv").getPath
        flinkDDFManager.sql2txt("load '" + filePath + "' into year_names")
        ddf = flinkDDFManager.getDDFByName("year_names")
    }
    ddf
  }

}
