package io.flink.ddf.etl

import java.util

import io.ddf.DDF
import io.ddf.content.Schema
import io.ddf.content.Schema.DataFormat
import io.ddf.etl.ASqlHandler

class SqlHandler(ddf: DDF) extends ASqlHandler(ddf) {
  override def sql2ddf(command: String): DDF = ???

  override def sql2ddf(command: String, schema: Schema): DDF = ???

  override def sql2ddf(command: String, dataFormat: DataFormat): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataSource: String): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataFormat: DataFormat): DDF = ???

  override def sql2ddf(command: String, schema: Schema, dataSource: String, dataFormat: DataFormat): DDF = ???

  override def sql2txt(command: String): util.List[String] = ???

  override def sql2txt(command: String, maxRows: Integer): util.List[String] = ???

  override def sql2txt(command: String, maxRows: Integer, dataSource: String): util.List[String] = ???
}
