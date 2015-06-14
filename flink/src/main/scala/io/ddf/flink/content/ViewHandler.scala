package io.ddf.flink.content

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.ViewHandler.{Column, Expression, OperationName, Operator}
import io.ddf.flink.utils.Samples
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row

import scala.collection.JavaConversions._


class ViewHandler(ddf: DDF) extends io.ddf.content.ViewHandler(ddf) {
  override def getRandomSample(numSamples: Int, withReplacement: Boolean, seed: Int): util.List[Array[AnyRef]] = {
    val dataSet:DataSet[Array[Object]] = ddf.getRepresentationHandler.get(RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString).asInstanceOf[DataSet[Array[Object]]]
    val count = dataSet.count
    val fraction = Samples.computeFractionForSampleSize(numSamples, count,withReplacement)
    seqAsJavaList(Samples.randomSample(dataSet,withReplacement,fraction,seed).collect())
  }

  override def getRandomSample(percent: Double, withReplacement: Boolean, seed: Int): DDF = {
    val dataSet:DataSet[Array[Object]] = ddf.getRepresentationHandler.get(RepresentationHandler.DATASET_ARR_OBJECT.getTypeSpecsString).asInstanceOf[DataSet[Array[Object]]]
    val newDS:DataSet[Array[Object]]  = Samples.randomSample(dataSet,withReplacement,percent,seed)
    val manager = ddf.getManager
    val columns = ddf.getSchema.getColumns
    val schema = new Schema(ddf.getSchemaHandler.newTableName(), columns)
    val sampleDDF = manager.newDDF(manager, newDS,Array(classOf[DataSet[_]], classOf[Array[Object]]), manager.getNamespace, null, schema)
    mLog.info(">>>>>>> adding ddf to DDFManager " + sampleDDF.getName)
    manager.addDDF(sampleDDF)
    sampleDDF.getMetaDataHandler.copyFactor(this.getDDF)
    sampleDDF
  }

  override def subset(columnExpr: util.List[Column], filter: Expression): DDF = {
    if (filter.isInstanceOf[Operator]) {
      val op = filter.asInstanceOf[Operator]
      if (op.getName == OperationName.grep || op.getName == OperationName.grep_ic) {
        throw new UnsupportedOperationException("Grep and Grep_IC are not supported in Flink DDF")
      }
    }
    super.subset(columnExpr, filter)
  }


}
