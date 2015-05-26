package io.flink.ddf

import io.ddf.DDFManager
import org.scalatest.{FlatSpec, Matchers}

class BaseSpec  extends FlatSpec with Matchers {
  val flinkDDFManager = DDFManager.get("flink").asInstanceOf[FlinkDDFManager]
  val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")

}
