package io.ddf.flink.etl

import io.ddf.DDF
import io.ddf.flink.BaseSpec
import io.ddf.flink.content.RepresentationHandler
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table._

import scala.collection.JavaConverters._

class SqlHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "create table and load data from file" in {
    val ddf = airlineDDF
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
    val ddf = airlineDDF
    val ddf1 = ddf.sql2ddf("select Year,Month from airline")
    val table = ddf1.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val collection: Seq[Row] = table.collect()
    val list = collection.asJava
    println(list)
    list.get(0).productArity should be(2)
    list.get(0).productElement(0).toString should startWith("200")
  }

  it should "run a sql command with where" in {
    val ddf = airlineDDF
    val ddf1 = ddf.sql2ddf("select Year,Month from airline where Year > 2008 AND Month > 1")
    val table = ddf1.getRepresentationHandler.get(classOf[Table]).asInstanceOf[Table]
    val collection: Seq[Row] = table.collect()
    val list = collection.asJava
    println(list)
    list.size should be(1)
    list.get(0).productArity should be(2)
    list.get(0).productElement(0) should be(2010)
  }

  it should "run a sql command with a join" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val ddf3 = ddf.sql2ddf("select Year,Month from airline join year_names on (Year = Year_num) ")
    val collection: Seq[Row] = ddf3.getRepresentationHandler.get(RepresentationHandler.TABLE_TYPE_SPECS: _*).asInstanceOf[Table].collect
    println(collection.mkString("\n"))

    val ddf4 = ddf.sql2ddf("select Year,Month from airline left join year_names on (Year = Year_num) ")
    val collection2: Seq[Row] = ddf4.getRepresentationHandler.get(RepresentationHandler.DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]].collect
    println(collection2.mkString("\n"))

  }

  it should "run a sql command with a join and where" in {
    val ddf = airlineDDF
    val ddf4 = ddf.sql2ddf("select Year,Month from airline left join year_names on (Year = Year_num) where Year_num > 2008 ")
    val collection2: Seq[Row] = ddf4.getRepresentationHandler.get(RepresentationHandler.DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]].collect
    println(collection2.mkString("\n"))

  }

  it should "run a sql command with an orderby" in {
    val ddf = airlineDDF
    val ddf4 = ddf.sql2ddf("select Year,Month from airline order by Year DESC")
    val collection2: Seq[Row] = ddf4.getRepresentationHandler.get(RepresentationHandler.DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]].collect
    println(collection2.mkString("\n"))
    val list = collection2.asJava
    list.get(0).productArity should be(2)
    list.get(0).productElement(0) should be(2010)
  }

  it should "run a sql command with an orderby and limit" in {
    val ddf = airlineDDF
    val ddf4 = ddf.sql2ddf("select Year,Month from airline order by Year DESC limit 2")
    val collection2: Seq[Row] = ddf4.getRepresentationHandler.get(RepresentationHandler.DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]].collect
    println(collection2.mkString("\n"))
    val list = collection2.asJava
    list.size should be(2)
    list.get(0).productArity should be(2)
    list.get(0).productElement(0) should be(2010)
  }

  it should "run a sql command with a group-by and order-by and limit" in {
    val ddf = airlineDDF
    val ddf4 = ddf.sql2ddf("select Year,Month,Count(Cancelled) from airline group by Year,Month order by Year DESC limit 5")
    val collection2: Seq[Row] = ddf4.getRepresentationHandler.get(RepresentationHandler.DATASET_ROW_TYPE_SPECS: _*).asInstanceOf[DataSet[Row]].collect
    println(collection2.mkString("\n"))
  }


}
