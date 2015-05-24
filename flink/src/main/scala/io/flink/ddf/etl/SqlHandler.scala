package io.flink.ddf.etl

import java.util
import java.util.Collections

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.{Column, DataFormat}
import io.ddf.etl.ASqlHandler
import io.flink.ddf.FlinkDDFManager
import io.flink.ddf.content.SchemaHandler
import io.flink.ddf.utils.{Load, Create}
import org.apache.flink.api.scala.DataSet
import org.apache.flink.api.table.Row
import scala.collection.JavaConversions._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}


class SqlHandler(theDDF:DDF) extends ASqlHandler(theDDF){
  override def sql2ddf(command: String): DDF = ???

  override def sql2ddf(command: String, schema: Schema): DDF = ???

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataSource: String): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: DataFormat): DDF = ???

  override def sql2txt(command: String): util.List[String] = {
    val schemaHandler:SchemaHandler= this.getDDF.getSchemaHandler.asInstanceOf[SchemaHandler]
    val fn = schemaHandler.parse(command)
    fn match {
      case c: Create  =>
        val cols:util.List[Column] = seqAsJavaList(c.columns.map(i=> new Column(i._1,i._2)).toSeq)
        val schema:Schema = new Schema(c.tableName,cols.toList)
        val ddf = this.getManager.newDDF(null, Array(classOf[String]), null, c.tableName, schema)
        this.getManager.addDDF(ddf)
        Collections.singletonList("0")

      case l:Load =>
        val ddf = this.getManager.getDDF(l.tableName)
        val env = this.getManager.asInstanceOf[FlinkDDFManager].getExecutionEnvironment
        val dataSet = env
          .readTextFile(l.url)
          .map(_.split(",").map(_.asInstanceOf[Object]))
        val typeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Array[Object]])
        val getTypeSpecs: Array[Class[_]] = Array(classOf[DataSet[_]], classOf[Row])
        ddf.getRepresentationHandler.set(dataSet,typeSpecs:_*)
        val rep:DataSet[Row] = ddf.getRepresentationHandler.get(getTypeSpecs:_*).asInstanceOf[DataSet[Row]]
        seqAsJavaList(rep.collect().map(_.toString()))
    }
  }

  override def sql2txt(command: String, maxRows: Integer): util.List[String] = sql2txt(command)

  override def sql2txt(command: String, maxRows: Integer, dataSource: String): util.List[String] = sql2txt(command)


}
