package io.flink.ddf.etl

import java.util.Collections

import io.ddf.{DDFManager, DDF}
import io.ddf.etl.Types.JoinType
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row
import scala.collection.JavaConversions._

class JoinHandlerSpec extends SqlHandlerSpec {

  it should "join tables" in {
    val manager = DDFManager.get("flink")
    val ddf: DDF = loadDDF(manager)
    val ddf2: DDF = loadDDF2(manager)
    val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println(i.toString()))
    val list = seqAsJavaList(collection)
    val first = list.get(0).toString
    first.contains("Tiger") should be(true)
    first.contains("2010") should be(true)
    val joinedDDF2 = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year_num"), Collections.singletonList("Year"))
    val rep2 = joinedDDF2.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection2 = rep2.collect()
    collection2.foreach(i => println(i.toString()))
  }
}
