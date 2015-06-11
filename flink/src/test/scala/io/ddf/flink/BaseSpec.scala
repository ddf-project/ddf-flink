package io.ddf.flink

import io.ddf.etl.IHandleMissingData.Axis
import io.ddf.{DDF, DDFManager}
import org.scalatest.{FlatSpec, Matchers}

class BaseSpec extends FlatSpec with Matchers {
  val flinkDDFManager = DDFManager.get("flink").asInstanceOf[FlinkDDFManager]
  var lDdf: DDF = null

  def ddf = loadDDF()

  def loadDDF(): DDF = {
    if (lDdf == null)
      lDdf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    lDdf
  }

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

  def loadAirlineNADDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = flinkDDFManager.getDDFByName("airlineWithNA")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql2txt("create table airlineWithNA (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airlineWithNA.csv").getPath
        flinkDDFManager.sql2txt("load '" + filePath + "' NO DEFAULTS into airlineWithNA")
        ddf = flinkDDFManager.getDDFByName("airlineWithNA")
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

  def loadMtCarsDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = flinkDDFManager.getDDFByName("mtcars")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql2txt("CREATE TABLE mtcars ("
          + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
          + ")")
        val filePath = getClass.getResource("/mtcars").getPath
        flinkDDFManager.sql2txt("load '" + filePath + "'  delimited by ' '  into mtcars")
        ddf = flinkDDFManager.getDDFByName("mtcars")
    }
    ddf
  }

}
