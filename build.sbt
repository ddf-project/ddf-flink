import Common._

organization := "io.ddf"

name := "ddf"

version := ddfVersion

retrieveManaged := true // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed

scalaVersion := theScalaVersion

scalacOptions := Seq("-unchecked", "-optimize", "-deprecation")

// Fork new JVMs for tests and set Java options for those
fork in Test := true

parallelExecution in ThisBuild := false

javaOptions in Test ++= Seq("-Xmx2g")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

conflictManager := ConflictManager.strict

commonSettings

lazy val root = project.in(file(".")).aggregate(flink, flinkExamples)

val excludeBreeze = ExclusionRule(organization = "org.scalanlp", name = "*")

val com_adatao_unmanaged = Seq(
  "com.adatao.unmanaged.net.rforge" % "REngine" % "2.1.1.compiled",
  "com.adatao.unmanaged.net.rforge" % "Rserve" % "1.8.2.compiled"
)

lazy val flink = project.in(file("flink")).settings(commonSettings: _*).settings(
  name := "ddf-flink",
  pomExtra := submodulePom,
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
    "com.clearspring.analytics" % "stream" % "2.7.0" exclude("asm", "asm"),
    "asm" % "asm" % "3.2",
    "org.scalatest" % "scalatest_2.10" % "2.2.2" % "test"
  ) ++ com_adatao_unmanaged,
  testOptions in Test += Tests.Argument("-oD")
)

lazy val flinkExamples = project.in(file("flink-examples")).dependsOn(flink).settings(commonSettings: _*).settings(
  name := "flink-examples",
  pomExtra := submodulePom
)

publishMavenStyle := true
