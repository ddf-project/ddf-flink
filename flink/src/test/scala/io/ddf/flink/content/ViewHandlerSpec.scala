package io.ddf.flink.content

import io.ddf.DDF
import io.ddf.exception.DDFException
import io.ddf.flink.BaseSpec
import scala.collection.JavaConversions._

class ViewHandlerSpec extends BaseSpec {
  val airlineDDF = loadAirlineDDF()
  val yearNamesDDF = loadYearNamesDDF()

  it should "project after remove columns " in {
    val ddf = airlineDDF
    val ddf0 = flinkDDFManager.sql2ddf("select * from airline", false)
    val yearLabel = "Year"
    val depTimeLabel = "DepTime"
    val columns: java.util.List[String] = List(yearLabel, depTimeLabel, "Month")

    val newddf1: DDF = ddf.VIEWS.removeColumn(yearLabel)
    newddf1.getNumColumns should be(28)
    val newddf2: DDF = ddf.VIEWS.removeColumns(depTimeLabel)
    val newddf3: DDF = ddf0.VIEWS.removeColumns(columns)
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

  it should "test sample with percentage" in {
    val sample = ddf.VIEWS.getRandomSample(0.5, false, 1)
    sample.VIEWS.head(3) should have size(3)
  }


  it should "get top 3 rows" in {
    val sample = flinkDDFManager.sql2ddf("SELECT Month from airline")
    flinkDDFManager.setDDFName(sample, "sample")
    sample.VIEWS.head(3) should have size(3)
  }
}
