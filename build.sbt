import Common._

name := "ddf-flink-root"

organization := "io.ddf"

version := ddfVersion

lazy val root = project.in(file(".")).aggregate(flink, flinkExamples)

lazy val flink = project.in(file("flink"))

lazy val flinkExamples = project.in(file("flink-examples")).dependsOn(flink)

