package io.flink.ddf.content

import io.ddf.DDF
import io.flink.ddf.content.SqlSupport._

class SchemaHandler(theDDF: DDF) extends io.ddf.content.SchemaHandler(theDDF) {

  val parser = new TableDdlParser

  def parse(input: String): Function = parser.parse(input)




}

