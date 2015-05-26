package io.flink.ddf.analytics

import io.ddf.analytics.AStatisticsSupporter
import io.flink.ddf.BaseSpec

class StatisticsHandlerSpec extends BaseSpec{

  it should "calculate summary" in {
    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    //mean:1084.26 stdev:999.14 var:998284.8 cNA:0 count:31 min:4.0 max:3920.0
    val randomSummary = summaries(9)
    randomSummary.variance() should be (998284.8 +- 1.0)
  }

  it should "calculate vector mean" in {
    ddf.getVectorMean("V1") should not be null
  }

  //TODO
 /* it should "calculate vector cor" in {
    ddf.getVectorCor("V1", "V2") should not be null
  }*/

  //TODO
  /*it should "calculate vector covariance" in {
    ddf.getVectorCovariance("V1", "V2") should not be null
  }*/

  it should "calculate vector variance" in {
    val variance = ddf.getVectorVariance("V1")
    variance.length should be(2)
  }

  it should "calculate vector quantiles" in {
    val pArray: Array[java.lang.Double] = Array(0.3, 0.5, 0.7)
    val expectedQuantiles: Array[java.lang.Double] = Array(801.0, 1416.0, 1644.0)
    val quantiles: Array[java.lang.Double] = ddf.getVectorQuantiles("V5", pArray)
    quantiles should equal(expectedQuantiles)
  }

  it should "calculate vector histogram" in {
    val bins: java.util.List[AStatisticsSupporter.HistogramBin] = ddf.getVectorHistogram("V15", 5)
    bins.size should be(5)
    val first = bins.get(0)
    first.getX should be (-24)
    first.getY should be (10.0)
  }
}
