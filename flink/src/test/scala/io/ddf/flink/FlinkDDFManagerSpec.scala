package io.ddf.flink

import io.ddf.DDFManager
import org.scalatest.{FlatSpec, Matchers}

class FlinkDDFManagerSpec extends BaseSpec {

  it should "load data from file" in {
    ddf.getNamespace should be("FlinkDDF")
    ddf.getColumnNames should have size (29)

  }

  it should "be addressable via URI" in {
    ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
    flinkDDFManager.getDDFByURI("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
  }

}
