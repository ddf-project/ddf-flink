package io.ddf.flink

class FlinkDDFSpec extends BaseSpec{

  it should "copy DDF" in {
    val ddf1 = loadMtCarsDDF()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
      col => ddf1.getSchemaHandler.setAsFactor(col)
    }

    val ddf2 = ddf1.copy()
    Array("cyl", "hp", "vs", "am", "gear", "carb").foreach {
      col =>
        ddf2.getSchemaHandler.getColumn(col).getOptionalFactor should not be(null)
    }
    ddf1.getNumRows should be(ddf2.getNumRows)
    ddf1.getNumColumns should be(ddf2.getNumColumns)
    ddf1.getName should not be(ddf2.getName)
  }

}
