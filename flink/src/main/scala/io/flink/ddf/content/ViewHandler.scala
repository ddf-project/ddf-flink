package io.flink.ddf.content

import io.ddf.DDF
import io.ddf.content.{IHandleViews, ViewHandler => CoreViewHandler}

class ViewHandler(DDF: DDF) extends CoreViewHandler(DDF) with IHandleViews {

}
