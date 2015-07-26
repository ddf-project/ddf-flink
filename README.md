DDF with Flink
===
This project depends on DDF and uses Apache Flink engine.

DDF
===

Distributed DataFrame: Productivity = Power x Simplicity
For Big Data Scientists & Engineers

* [Visit DDF Website!](http://ddf.io)

* [Wiki](https://github.com/ddf-project/DDF/wiki)

* [Issues tracker](https://github.com/ddf-project/DDF/issues)

* [Questions/Comments/Feature Requests](https://groups.google.com/forum/#!forum/ddf-project)

---

### Getting Started

This project depends on DDF and requires its installation to run.

Before installing DDF, update line#482 in project/RootBuild.scala to,
```
  ),

publishArtifact in (Compile, packageDoc) := false
```
This is to avoid the error in publishing docs through SBT.


DDF can be installed by,

```
$ git clone git@github.com:ddf-project/DDF.git
$ cd DDF
$ git fetch
$ git checkout -v1.2.0
$ bin/run-once.sh
$ sbt publishLocal
```

Installing `ddf-with-flink` can be done by

```
$ git clone git@github.com:tuplejump/ddf-with-flink.git
$ cd ddf-with-flink
$ bin/run-once.sh
$ mvn package install -DskipTests
```

### Running tests
Tests can be run either through SBT or Maven,
```
$ sbt test
$ mvn test

//running a single test

$ sbt "testOnly *FlinkDDFManagerSpec*"

$ mvn test -Dsuites='io.ddf.flink.FlinkDDFManagerSpec'
```

### Starting `ddf-shell` with `flink` engine

Execute the following only after installing `ddf-with-flink`
```
$ bin/ddf-shell
```

### Running the example,
```
$ bin/run-flink-example io.ddf.flink.examples.FlinkDDFExample
```
