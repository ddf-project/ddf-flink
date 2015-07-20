import Common._

name := "ddf-flink"

organization := "io.ddf"

version := ddfVersion

val excludeBreeze= ExclusionRule(organization = "org.scalanlp", name = "*")

libraryDependencies ++= Seq(
  "io.ddf" %% "ddf_core" % ddfVersion  ,
  "org.apache.flink" % "flink-core" % flinkVersion ,
  "org.apache.flink" % "flink-java" % flinkVersion ,
  "org.apache.flink" % "flink-scala" % flinkVersion ,
  "org.apache.flink" % "flink-clients" % flinkVersion ,
  "org.apache.flink" % "flink-table" % flinkVersion ,
  "org.apache.flink" % "flink-runtime" % flinkVersion ,
  "org.apache.flink" % "flink-optimizer" % flinkVersion ,
  "com.univocity" % "univocity-parsers" % "1.5.5",
  "org.apache.flink" % "flink-ml" % flinkVersion excludeAll(excludeBreeze)
)


