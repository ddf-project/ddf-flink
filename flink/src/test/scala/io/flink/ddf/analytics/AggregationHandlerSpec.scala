package io.flink.ddf.analytics

import io.ddf.DDFManager
import io.ddf.types.AggregateTypes.AggregateFunction
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConversions._

class AggregationHandlerSpec extends FlatSpec with Matchers {

  val flinkDDFManager = DDFManager.get("flink")
  val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")

  it should "calculate simple aggregates" in {
    val aggregateResult = ddf.aggregate("V1, V2, min(V15), max(V16)")
    val result: Array[Double] = aggregateResult.get("2010,3")
    result.length should be(2)

    val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "V1")
    colAggregate should be(2010)

    val ddf2 = ddf.getAggregationHandler.agg(List("mean=avg(V15)"))
    val rowDataSet = ddf2.getRepresentationHandler.get(classOf[DataSet[_]], classOf[Row]).asInstanceOf[DataSet[Row]]
    val row: Row = rowDataSet.first(1).collect().head
    row.productElement(0) should be(9)
  }

  it should "group data" in {
    val l1: java.util.List[String] = List("V3")
    val l2: java.util.List[String] = List("mean(V16)")

    val avgDelayByDay = ddf.groupBy(l1, l2)
    avgDelayByDay.getColumnNames should (contain("AVG(V16)") and contain("V3"))
  }

  it should "calculate correlation" in {
    //0.8977184691827954
    ddf.correlation("V15", "V16") should be >= (0.89)
  }
}
