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

This project depends on DDF v1.2.0 and requires its installation to run. To get DDF version 1.2.0, clone DDF repo and checkout the v1.2.0 tag.

```
$ git clone git@github.com:ddf-project/DDF.git
$ cd DDF
$ git fetch
$ git checkout -v1.2.0
```

No changes are required when installing DDF using maven.

Before installing DDF using SBT, add a new line after line#482 in project/RootBuild.scala, (don't miss adding the comma at the end of line#482)

```
  ),

publishArtifact in (Compile, packageDoc) := false
```
This is to avoid the error in publishing docs through SBT.



DDF can be installed by,

```
$ bin/run-once.sh
//using maven
$ mvn package install -DskipTests
//or using sbt
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
$ sbt package
$ bin/ddf-shell
```

SBT package is required since it generates the `lib_managed` which is required for running the scripts.

### Running the example,
```
$ sbt package
$ bin/run-flink-example io.ddf.flink.examples.FlinkDDFExample
```

SBT package is required since it generates the `lib_managed` which is required for running the scripts.
