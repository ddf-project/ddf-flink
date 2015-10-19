package io.ddf.flink

import io.ddf.DDFManager
import io.ddf.flink.ml.FlinkMLFacade
import org.apache.flink.ml.clustering.KMeans
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class DemoSpec extends FlatSpec with Matchers {
  it should "run all commands" in {


    val mgr = DDFManager.get(FlinkConstants.ENGINE_NAME)

    val filePath = getClass.getResource("/airline.csv").getPath
//    val filePath = "/home/shiti/work/adatao/ddf-with-flink/resources/test/airline.csv"
    mgr.sql("create table airline (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int,  TailNum string, ActualElapsedTime int, CRSElapsedTime int,  AirTime int, ArrDelay int, DepDelay int, Origin string,  Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,  CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )", FlinkConstants.ENGINE_NAME)
    mgr.sql(s"load '$filePath' into airline", FlinkConstants.ENGINE_NAME)

    //# Table like
    val table = mgr.sql2ddf("select * from airline")

    table.getNumRows
    table.getNumColumns
    table.getColumnNames

    val table2 = table.VIEWS.project("ArrDelay", "DepDelay", "Origin", "DayOfWeek", "Cancelled")
    table2.getColumnNames

    val table3 = table2.sql("select * from @this where Origin='ISP'", "")
    table3.getRows

    val table4 = table2.groupBy(List("Origin"), List("adelay=avg(ArrDelay)"))
    table4.getColumnNames should contain("adelay")
    table4.VIEWS.top(10, "adelay", "asc")


    //# R Dataframe: xtabs, quantile, histogram
    val statsTable = table2.VIEWS.project("ArrDelay", "DepDelay", "DayOfWeek", "Cancelled")
    statsTable.getSummary
    statsTable.getFiveNumSummary
    val table5 = table.binning("Distance", "EQUALINTERVAL", 3, null, true, true)
    val table6 = table2.Transform.transformScaleMinMax

    //# Not MR
    statsTable.setMutable(true)
    statsTable.getSummary
    statsTable.dropNA()
    statsTable.getSummary

    // # ML
    val mlData = table.VIEWS.project("ArrDelay", "DepDelay")
    val imodel = mlData.ML.asInstanceOf[FlinkMLFacade].kMeans(Option(3), Option(5))
    val kmeans: KMeans = imodel.getRawModel.asInstanceOf[KMeans]
    kmeans.centroids.get.collect().flatten.foreach(println)

    // # Data Colab + Multi Languages
    mgr.setDDFName(table2, "flightInfo")
    val flightTable = mgr.getDDFByName("flightInfo")
    flightTable.getColumnNames
  }
}
