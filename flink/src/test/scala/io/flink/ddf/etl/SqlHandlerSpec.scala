package io.flink.ddf.etl

import io.ddf.{DDF, DDFManager}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class SqlHandlerSpec extends FlatSpec with Matchers {
  it should "create table and load data from file" in {
    val manager = DDFManager.get("flink")
    val ddf: DDF = loadDDF(manager)
    ddf.getColumnNames should have size (29)

    //MetaDataHandler
    ddf.getNumRows should be(31)

    //StatisticsComputer
    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    //mean:1084.26 stdev:999.14 var:998284.8 cNA:0 count:31 min:4.0 max:3920.0
    val randomSummary = summaries(9)
    randomSummary.variance() >= 998284
  }

  it should "run a simple sql command" in {
    val manager = DDFManager.get("flink")
    val ddf: DDF = loadDDF(manager)
    val ddf1 = ddf.sql2ddf("select Year,Month from airline")
    val table = ddf1.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val collection: Seq[Row] = table.toSet[Row].collect()
    val list = collection.asJava
    println(list)
    list.get(0).productArity should be(2)
    list.get(0).productElement(0).toString should startWith ("200")
  }

  it should "run a sql command with where" in {
    val manager = DDFManager.get("flink")
    val ddf: DDF = loadDDF(manager)
    val ddf1 = ddf.sql2ddf("select Year,Month from airline where Year > 2008 AND Month > 1")
    val table = ddf1.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val collection: Seq[Row] = table.toSet[Row].collect()
    val list = collection.asJava
    println(list)
    list.size should  be (1)
    list.get(0).productArity should be(2)
    list.get(0).productElement(0) should be(2010)
  }


  def loadDDF(manager: DDFManager): DDF = {

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int," + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int," + "CRSArrTime int,UniqueCarrier string, FlightNum int, " + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, " + "AirTime int, ArrDelay int, DepDelay int, Origin string, " + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, " + "CancellationCode string, Diverted string, CarrierDelay int, " + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )")
    val filePath = getClass.getResource("/airline.csv").getPath
    manager.sql2txt("load '" + filePath + "' into airline")
    val ddf = manager.getDDF("airline")
    ddf
  }

  def loadDDF2(manager: DDFManager): DDF = {
    manager.sql2txt("create table year_names (Year_num int,Name string)")
    val filePath = getClass.getResource("/year_names.csv").getPath
    manager.sql2txt("load '" + filePath + "' into year_names")
    val ddf = manager.getDDF("year_names")
    ddf
  }

}
