package io.flink.ddf

import io.ddf.DDFManager
import org.scalatest.{FlatSpec, Matchers}

class FlinkRowDDFManagerSpec extends FlatSpec with Matchers {
  it should "load data from file" in {
    val flinkDDFManager = DDFManager.get("flink-row")
    val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    ddf.getNamespace should be("flinkRowDDF")
    ddf.getColumnNames should have size(29)

    //MetaDataHandler
    ddf.getNumRows should be(31)


    /*val ddf2 = ddf.VIEWS.project("V1", "V2")
    ddf.getColumnNames should have size(2)*/

    //assert(ddf.getSummary.length == 2)
  }

}
