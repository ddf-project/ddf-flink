package io.flink.ddf

import io.ddf.DDFManager
import org.scalatest.{FlatSpec, Matchers}

class FlinkDDFManagerSpec extends FlatSpec with Matchers {

  it should "load data from file" in {
    val flinkDDFManager = DDFManager.get("flink")
    val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    ddf.getNamespace should be("FlinkDDF")
    ddf.getColumnNames should have size (29)

    ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
    flinkDDFManager.getDDF("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
  }

}
