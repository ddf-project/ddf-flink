import Common._

name := "ddf-flink-root"

organization := "io.ddf"

version := ddfVersion

scalaVersion := "2.10.4"

lazy val root = project.in(file(".")).aggregate(flink, flinkExamples)

val excludeBreeze = ExclusionRule(organization = "org.scalanlp", name = "*")

val com_adatao_unmanaged = Seq(
  "com.adatao.unmanaged.net.rforge" % "REngine" % "2.1.1.compiled",
  "com.adatao.unmanaged.net.rforge" % "Rserve" % "1.8.2.compiled"
)

lazy val flink = project.in(file("flink")).settings(
  name := "ddf-flink",
  organization := "io.ddf",
  version := ddfVersion,
  libraryDependencies ++= Seq(
    "io.ddf" %% "ddf_core" % ddfVersion,
    "org.apache.flink" % "flink-core" % flinkVersion,
    "org.apache.flink" % "flink-java" % flinkVersion,
    "org.apache.flink" % "flink-scala" % flinkVersion,
    "org.apache.flink" % "flink-clients" % flinkVersion,
    "org.apache.flink" % "flink-table" % flinkVersion,
    "org.apache.flink" % "flink-runtime" % flinkVersion,
    "org.apache.flink" % "flink-optimizer" % flinkVersion,
    "com.univocity" % "univocity-parsers" % "1.5.5",
    "org.apache.flink" % "flink-ml" % flinkVersion excludeAll (excludeBreeze),
    "org.scalanlp" %% "breeze" % "0.11.2",
    "com.clearspring.analytics" % "stream" % "2.4.0" exclude("asm", "asm"),
    "asm" % "asm" % "3.2",
    "org.scalatest" % "scalatest_2.10" % "2.2.2" % "test"
  ) ++ com_adatao_unmanaged
)

lazy val flinkExamples = project.in(file("flink-examples")).dependsOn(flink).settings(
  name := "flink-examples",
  organization := "io.ddf",
  version := ddfVersion
)

resolvers ++= Seq("Adatao Mvnrepos Snapshots" at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
  "Adatao Mvnrepos Releases" at "https://raw.github.com/adatao/mvnrepos/master/releases")