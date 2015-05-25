package io.flink.ddf

import java.io.File

import io.ddf.content.APersistenceHandler.PersistenceUri
import io.ddf.misc.Config.ConfigConstant
import io.ddf.{DDF, DDFManager}
import junit.framework.Assert
import org.scalatest.{FlatSpec, Matchers}

class FlinkDDFManagerSpec extends FlatSpec with Matchers {

  it should "load data from file" in {
    val flinkDDFManager = DDFManager.get("flink")
    val ddf = flinkDDFManager.loadTable(getClass.getResource("/airline.csv").getPath, ",")
    ddf.getNamespace should be("FlinkDDF")
    ddf.getColumnNames should have size (29)

    //MetaDataHandler
    ddf.getNumRows should be(31)

    //StatisticsComputer
    val summaries = ddf.getSummary
    summaries.head.max() should be(2010)

    //mean:1084.26 stdev:999.14 var:998284.8 cNA:0 count:31 min:4.0 max:3920.0
    val randomSummary = summaries(9)
    randomSummary.variance() >= 998284
    ddf.getUri should be("ddf://"+ddf.getNamespace+"/" + ddf.getName)
    flinkDDFManager.getDDF("ddf://"+ddf.getNamespace+"/" + ddf.getName) should be(ddf)
  }

  it should "persist and unpersist a flink DDF" in {
    val manager: DDFManager = DDFManager.get("flink")
    val ddf: DDF = manager.newDDF
    val uri: PersistenceUri = ddf.persist
    uri.getEngine should be(ConfigConstant.ENGINE_NAME_FLINK.toString)
    new File(uri.getPath).exists should be(true)
    ddf.unpersist
  }
}
