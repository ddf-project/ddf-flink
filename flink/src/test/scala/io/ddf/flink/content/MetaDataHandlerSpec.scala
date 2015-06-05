package io.ddf.flink.content

import io.ddf.flink.BaseSpec

class MetaDataHandlerSpec extends BaseSpec {
  it should "get number of rows" in {
    ddf.getNumRows should be(31)
  }
}
