package io.ddf.flink

import io.ddf.{DDF, DDFManager}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class BaseSpec extends FlatSpec with Matchers {
  val flinkDDFManager = DDFManager.get("").asInstanceOf[FlinkDDFManager]
  var lDdf: DDF = null

  def ddf = loadDDF()

  def loadDDF(): DDF = {
    val filePath: String = getClass.getResource("/airline.csv").getPath
    lDdf = Option(lDdf).getOrElse(flinkDDFManager.loadTable(filePath, ","))
    lDdf
  }

  private def loadCSVIfNotExists(ddfName: String,
                                 fileName: String,
                                 columns: Seq[String],
                                 delimiter: Char = ',',
                                 isNullSetToDefault: Boolean = true): DDF = {
    try {
      flinkDDFManager.getDDFByName(ddfName)
    } catch {
      case e: Exception =>
        flinkDDFManager.sql(s"create table $ddfName (${columns.mkString(",")})", FlinkConstants.ENGINE_NAME)
        val filePath = getClass.getResource(fileName).getPath
        val additionalOptions = if (!isNullSetToDefault) {
          "WITH NULL '' NO DEFAULTS"
        } else {
          s"DELIMITED BY '$delimiter'"
        }
        flinkDDFManager.sql(s"load '$filePath' $additionalOptions INTO $ddfName", FlinkConstants.ENGINE_NAME)
        flinkDDFManager.getDDFByName(ddfName)
    }
  }

  def loadIrisTrain(): DDF = {
    loadCSVIfNotExists("iris", "/fisheriris.csv", Seq("flower double", "petal double", "septal double"))
  }

  private val airlineColumns = Seq("Year int", "Month int", "DayofMonth int", "DayOfWeek int", "DepTime int",
    "CRSDepTime int", "ArrTime int", "CRSArrTime int", "UniqueCarrier string", "FlightNum int", "TailNum string",
    "ActualElapsedTime int", "CRSElapsedTime int", "AirTime int", "ArrDelay int", "DepDelay int", "Origin string",
    "Dest string", "Distance int", "TaxiIn int", "TaxiOut int", "Cancelled int", "CancellationCode string",
    "Diverted string", "CarrierDelay int", "WeatherDelay int", "NASDelay int", "SecurityDelay int", "LateAircraftDelay int")

  def loadIrisTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("iris")
    //train.sql2ddf("SELECT petal, septal FROM iris WHERE flower = 1.0000")
    train.VIEWS.project("petal", "septal")
  }

  def loadAirlineDDF(): DDF = {
    val ddfName = "airline"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", airlineColumns)
  }

  def loadAirlineDDFWithoutDefault(): DDF = {
    val ddfName = "airline"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", airlineColumns, isNullSetToDefault = false)
  }

  def loadAirlineNADDF(): DDF = {
    val ddfName = "airlineWithNA"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", airlineColumns, isNullSetToDefault = false)
  }


  def loadYearNamesDDF(): DDF = {
    val ddfName = "year_names"
    loadCSVIfNotExists(ddfName, s"/$ddfName.csv", Seq("Year_num int", "Name string"))
  }

  def loadMtCarsDDF(): DDF = {
    val ddfName: String = "mtcars"
    loadCSVIfNotExists(ddfName, s"/$ddfName",
      Seq("mpg double", "cyl int", "disp double", "hp int", "drat double", "wt double",
        "qsec double", "vs int", "am int", "gear int", "carb int"),
      delimiter = ' ')
  }

  def loadRegressionTrain(): DDF = {
    val ddfName: String = "regression_data"
    loadCSVIfNotExists(ddfName, "/regressionData.csv", Seq("col1 double", "col2 double"))
  }

  def loadRegressionTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("regression_data")
    train.VIEWS.project("col2")
  }

  def loadRatingsTrain(): DDF = {
    loadCSVIfNotExists("user_ratings", "/ratings.csv", Seq("user_id int", "item_id int", "rating double"))
  }

  def loadRatingsTest(): DDF = {
    val train = flinkDDFManager.getDDFByName("user_ratings")
    train.VIEWS.project("user_id", "item_id")
  }
}
