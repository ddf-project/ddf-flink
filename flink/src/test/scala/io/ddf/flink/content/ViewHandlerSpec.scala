package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.flink.BaseSpec

class ViewHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "project after remove columns " in {
    val ddf = airlineDDF
    val columns: java.util.List[String] = new java.util.ArrayList()
    columns.add("year")
    columns.add("month")
    columns.add("deptime")

    val newddf1: DDF = ddf.VIEWS.removeColumn("year")
    val newddf2: DDF = ddf.VIEWS.removeColumns("year", "deptime")
    val newddf3: DDF = ddf.VIEWS.removeColumns(columns)
    newddf1.getNumColumns should be(28)
    newddf2.getNumColumns should be(27)
    newddf3.getNumColumns should be(26)
  }


  it should "test sample" in {
    val ddf = loadMtCarsDDF()
    val sample = ddf.VIEWS.getRandomSample(10)

    sample.get(0)(0).asInstanceOf[Double] should not be (sample.get(1)(0).asInstanceOf[Double])
    sample.get(1)(0).asInstanceOf[Double] should not be (sample.get(2)(0).asInstanceOf[Double])
    sample.get(2)(0).asInstanceOf[Double] should not be (sample.get(3)(0).asInstanceOf[Double])
    sample.size should be(10)
  }
}
