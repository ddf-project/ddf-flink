package io.flink.ddf.etl

import java.util.Collections

import io.ddf.DDF
import io.ddf.etl.Types.JoinType
import io.flink.ddf.BaseSpec
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._

class JoinHandlerSpec extends BaseSpec {

  it should "inner join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, null, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println("[" + i + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
    val first = list.get(0)
    first.productArity should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("Name") should be(true)

  }

  it should "left semi join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.LEFTSEMI, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println("[" + i + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(2) // only 2 values i.e 2008 and 2010 have values in both tables
    val first = list.get(0)
    first.productArity should be(29) //only left columns should be fetched
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("Name") should be(false)

  }

  it should "left outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.LEFT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println("[" + i + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(3) // 3 distinct values in airline years 2008,2009,2010
    val first = list.get(0)
    first.productArity should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("Name") should be(true)
  }

  it should "right outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.RIGHT, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println("[" + i + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(4) // 4 distinct values in airline years 2007,2008,2010,2011
    val first = list.get(0)
    first.productArity should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("Name") should be(true)
  }

  it should "full outer join tables" in {
    val ddf: DDF = airlineDDF
    val ddf2: DDF = yearNamesDDF
    val joinedDDF = ddf.join(ddf2, JoinType.FULL, null, Collections.singletonList("Year"), Collections.singletonList("Year_num"))
    val rep = joinedDDF.getRepresentationHandler.get(Array(classOf[DataSet[_]], classOf[Row]): _*).asInstanceOf[DataSet[Row]]
    val collection = rep.collect()
    collection.foreach(i => println("[" + i + "]"))
    val list = seqAsJavaList(collection)
    list.size should be(5) //over all 5 distinct years 2007 - 2011 across both tables
    val first = list.get(0)
    first.productArity should be(31) //29 columns in first plus 2 in second
    val colNames = joinedDDF.getSchema.getColumnNames
    colNames.contains("Year") should be(true)
    //check if the names from second ddf have been added to the schema
    colNames.contains("Name") should be(true)

  }
}
