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

This project depends on DDF core and requires its installation to run. To get DDF core, clone DDF repo and checkout the master branch.

```
$ git clone git@github.com:ddf-project/DDF.git
$ cd DDF
```

DDF can be installed by,

```
$ sbt publishLocal
```

Installing `ddf-with-flink` can be done by

```
$ git clone git@github.com:ddf-project/ddf-flink.git
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

####Todo

1. Test the ML method `getConfusionMatrix`
2. Implement `transformPython` and `flattenDDF` for TransformationHandler and also test the R functions.
3. Implement the methods `r2score`, `residuals`, `roc` and `rmse` for MLMetricsSupporter
