package io.ddf.flink.utils

object DemoSupport {
  lazy val isDemoMode: Boolean = {
    val enDemo = System.getenv("DEMO_MODE")
    val propDemo = System.getProperty("DEMO_MODE")
    if((enDemo != null && enDemo.equalsIgnoreCase("TRUE")) || (propDemo != null && enDemo.equalsIgnoreCase("TRUE"))){
      true
    } else {
      false
    }
  }
}
