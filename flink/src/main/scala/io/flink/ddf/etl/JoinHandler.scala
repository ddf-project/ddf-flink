package io.flink.ddf.etl

import java.util

import io.ddf.DDF
import io.ddf.etl.IHandleJoins
import io.ddf.etl.Types.JoinType
import io.ddf.misc.ADDFFunctionalGroupHandler

class JoinHandler(ddf: DDF) extends ADDFFunctionalGroupHandler(ddf) with IHandleJoins {
  override def join(anotherDDF: DDF, joinType: JoinType, byColumns: util.List[String], byLeftColumns: util.List[String], byRightColumns: util.List[String]): DDF = ???
}
