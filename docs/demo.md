The commands in the file should be executed from the project's base directory

start shell

```
bin/ddf-shell
```

create DDFManager using flink

```
import  io.ddf.DDFManager;
DDFManager manager = DDFManager.get("flink");
```

create mtcars table and load data

```
manager.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string)");
manager.sql("load 'resources/test/mtcars' delimited by ' '  into mtcars");
```

create airline table and load data
```
manager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int,  TailNum string, ActualElapsedTime int, CRSElapsedTime int,  AirTime int, ArrDelay int, DepDelay int, Origin string,  Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,  CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )");
manager.sql("load 'resources/test/airlineWithNA.csv' WITH NULL '' NO DEFAULTS into airlineWithNA");
```

create ddf using sql2ddf
```
DDF ddf = manager.sql2ddf("select * from mtcars");
```

basic statistics
```
int rowCount = ddf.getNumRows();
print(rowCount);
print(ddf.getColumnNames());

s= ddf.getSummary();
print(s[0]);

aggrSum = ddf.aggregate("vs, carb, sum(mpg)");
print(aggrSum.get("1,1"));

aggrMean = ddf.aggregate("vs, carb, mean(mpg)");
print(aggrMean.get("1,1"));

aggrComb = ddf.aggregate("vs, am, sum(mpg), min(hp)");
print(aggrComb.get("1,1"));

vectorVariance = ddf.getVectorVariance("mpg");
print(vectorVariance);

vectorMean = ddf.getVectorMean("mpg");
print(vectorMean);

```

projections
```
newDDF = ddf.VIEWS.project("mpg","wt");
print(newDDF.getColumnNames());
```

transformScale
```
DDF rescaledDDF = ddf.Transform.transformScaleMinMax();
print(rescaledDDF.getSummary()[0]);
```

dropNA
```
DDF airlineDDF = manager.sql2ddf("select * from airlineWithNA");
cleanData = airlineDDF.dropNA();

int rowCount = cleanData.getNumRows();
print(rowCount);

```

ml
```
import io.ddf.flink.ml.FlinkMLFacade;
import io.ddf.ml.IModel;

import org.apache.flink.ml.classification.SVM;
import scala.None$;
import scala.Option;

Option None = None$.MODULE$;

manager.sql("create table iris (flower double, petal double, septal double)");
manager.sql("load 'resources/test/fisheriris.csv' into iris");
DDF trainDDF = manager.getDDFByName("iris");
DDF testDDF = trainDDF.VIEWS.project("petal", "septal");

IModel imodel = ((FlinkMLFacade) trainDDF.ML).svm(None, None, None, None, None, None);

DDF result = testDDF.ML.applyModel(imodel);
print(result.getNumRows());
```