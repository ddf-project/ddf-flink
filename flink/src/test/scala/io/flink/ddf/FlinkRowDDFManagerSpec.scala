package io.flink.ddf

import io.ddf.DDFManager
import org.scalatest.{FlatSpec, Matchers}

class FlinkRowDDFManagerSpec extends FlatSpec with Matchers {

  it should "load data from file" in {
    val flinkDDFManager = DDFManager.get("flink-row")
    val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    ddf.getNamespace should be("flinkRowDDF")
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
}
