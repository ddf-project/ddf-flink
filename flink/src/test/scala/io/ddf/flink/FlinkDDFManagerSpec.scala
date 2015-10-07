package io.ddf.flink

class FlinkDDFManagerSpec extends BaseSpec {

  it should "load data from file" in {
    ddf.getNamespace should be("FlinkDDF")
    ddf.getColumnNames should have size (29)
  }

  it should "be addressable via URI" in {
    ddf.getUri should be("ddf://" + ddf.getNamespace + "/" + ddf.getName)
    flinkDDFManager.getDDFByURI("ddf://" + ddf.getNamespace + "/" + ddf.getName) should be(ddf)
  }

  it should "get DDF using SQL2DDF" in {
    loadAirlineDDF()
    val ddf = flinkDDFManager.sql2ddf("select * from airline", FlinkConstants.ENGINE_NAME_FLINK)
    flinkDDFManager.setDDFName(ddf, "awesome_ddf")
    val ddf1 = flinkDDFManager.getDDFByURI(ddf.getUri)
    assert(ddf1 != null)
    assert(ddf1.getNumRows == ddf.getNumRows)
  }

  it should "list DDF" in {
    loadAirlineDDF()
    loadMtCarsDDF()

    val listDDF = flinkDDFManager.listDDFs()

    listDDF.foreach {
      ddfinfo => println(s"uri = ${ddfinfo.getUri}; createdTime = ${ddfinfo.getCreatedTime}")
    }

    listDDF.count(ddf => Option(ddf.getName).isDefined) should be(4) //ddf loaded for all tests, awesome_ddf, airline, mtcars
  }

}
