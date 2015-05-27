package io.flink.ddf.analytics

import io.ddf.exception.DDFException
import io.ddf.types.AggregateTypes.AggregateFunction
import io.flink.ddf.BaseSpec

import scala.collection.JavaConversions._

class AggregationHandlerSpec extends BaseSpec {

  it should "calculate simple aggregates" in {
    val aggregateResult = ddf.aggregate("V1, V2, min(V15), max(V16)")
    val result: Array[Double] = aggregateResult.get("2010,3")
    result.length should be(2)

    val colAggregate = ddf.getAggregationHandler.aggregateOnColumn(AggregateFunction.MAX, "V1")
    colAggregate should be(2010)
  }

  it should "group data" in {
    val l1: java.util.List[String] = List("V3")
    val l2: java.util.List[String] = List("mean(V16)")

    val avgDelayByDay = ddf.groupBy(l1, l2)
    avgDelayByDay.getColumnNames should (contain("AVG(V16)") and contain("V3"))
  }

  it should "group and aggregate in 2 steps" in {
    val ddf2 = ddf.getAggregationHandler.groupBy(List("V3"))
    val result = ddf2.getAggregationHandler.agg(List("mean=avg(V15)"))
    result.getColumnNames should (contain("AVG(V15)") and contain("V3"))
  }

  it should "throw an error on aggregate without groups" in {
    intercept[DDFException] {
      ddf.getAggregationHandler.agg(List("mean=avg(V15)"))
    }
  }

  it should "calculate correlation" in {
    //0.8977184691827954
    ddf.correlation("V15", "V16") should be (0.89 +- 1)
  }
}
