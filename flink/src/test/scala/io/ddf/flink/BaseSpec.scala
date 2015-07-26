package io.ddf.flink

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

  def loadIrisTrain(): DDF = {
    try {
      flinkDDFManager.getDDFByName("iris")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql("create table iris (flower double, petal double, septal double)")
        val filePath = getClass.getResource("/fisheriris.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' into iris")
        flinkDDFManager.getDDFByName("iris")
    }
  }

  def loadIrisTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("iris")
    //train.sql2ddf("SELECT petal, septal FROM iris WHERE flower = 1.0000")
    train.VIEWS.project("petal", "septal")
  }

  def loadAirlineDDF(): DDF = {
    var ddf: DDF = null
    try {
      ddf = flinkDDFManager.getDDFByName("airline")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql("create table airline (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airline.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' into airline")
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
        flinkDDFManager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
        val filePath = getClass.getResource("/airlineWithNA.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' WITH NULL '' NO DEFAULTS into airlineWithNA")
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
        flinkDDFManager.sql("create table year_names (Year_num int,Name string)")
        val filePath = getClass.getResource("/year_names.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' into year_names")
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
        flinkDDFManager.sql("CREATE TABLE mtcars ("
          + "mpg double,cyl int, disp double, hp int, drat double, wt double, qsec double, vs int, am int, gear int, carb int"
          + ")")
        val filePath = getClass.getResource("/mtcars").getPath
        flinkDDFManager.sql("load '" + filePath + "'  delimited by ' '  into mtcars")
        ddf = flinkDDFManager.getDDFByName("mtcars")
    }
    ddf
  }

  def loadRegressionTrain(): DDF = {
    try {
      flinkDDFManager.getDDFByName("regression_data")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql("create table regression_data (col1 double, col2 double)")
        val filePath = getClass.getResource("/regressionData.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' into regression_data")
        flinkDDFManager.getDDFByName("regression_data")
    }
  }

  def loadRegressionTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("regression_data")
    train.VIEWS.project("col2")
  }

  def loadRatingsTrain(): DDF = {
    try {
      flinkDDFManager.getDDFByName("user_ratings")
    } catch {
      case e: Exception =>
        flinkDDFManager.sql("create table user_ratings (user_id int, item_id int,rating double)")
        val filePath = getClass.getResource("/ratings.csv").getPath
        flinkDDFManager.sql("load '" + filePath + "' into user_ratings")
        flinkDDFManager.getDDFByName("user_ratings")
    }
  }

  def loadRatingsTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("user_ratings")
    train.VIEWS.project("user_id", "item_id")
  }
}
