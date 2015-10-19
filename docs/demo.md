The commands in the file should be executed from the project's base directory

start shell

```
bin/ddf-shell
```

imports required for demo
```
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.SqlResult;
import io.ddf.flink.FlinkConstants;
```

create DDFManager using flink

```
DDFManager manager = DDFManager.get(FlinkConstants.ENGINE_NAME());
```

create airline table and load data

```
manager.sql("create table airline (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int,  TailNum string, ActualElapsedTime int, CRSElapsedTime int,  AirTime int, ArrDelay int, DepDelay int, Origin string,  Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,  CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )", FlinkConstants.ENGINE_NAME());
manager.sql("load 'resources/test/airline.csv'  into airline",FlinkConstants.ENGINE_NAME());
```

create ddf using sql2ddf
```
DDF table = manager.sql2ddf("select * from airline");
```

# Table like
```
int rowCount = table.getNumRows();
print(rowCount);
print(table.getNumColumns());
print(table.getColumnNames());

DDF table2 = table.VIEWS.project("ArrDelay", "DepDelay", "Origin", "DayOfWeek", "Cancelled");
print(table2.getColumnNames());

SqlResult table3 = table2.sql("select * from @this where Origin='ISP'", "");
print(table3.getRows().size());

DDF table4 = table2.groupBy(Arrays.asList("Origin"), Arrays.asList("adelay=avg(ArrDelay)"));
print(table4.getNumRows());
topRows = table4.VIEWS.top(2, "adelay", "asc");
print(topRows);
```

# R Dataframe: xtabs, quantile, histogram
```
DDF statsTable = table2.VIEWS.project("ArrDelay", "DepDelay", "DayOfWeek", "Cancelled");
s= statsTable.getSummary();
print(s[0]);

fiveNumSummary = statsTable.getFiveNumSummary();
print(fiveNumSummary[0]);

DDF table5 = table.binning("Distance", "EQUALINTERVAL", 3, null, true, true);
levels = table5.getColumn("Distance").getOptionalFactor().getLevelCounts();
print(levels);

DDF rescaledDDF = table2.Transform.transformScaleMinMax();
s= rescaledDDF.getSummary();
print(s[0]);
```

# Not MR
```
statsTable.setMutable(true);
s= statsTable.getSummary();
print(s[0]);

statsTable.dropNA();
s= statsTable.getSummary();
print(s[0]);
```
# Data Colab + Multi Languages
```
manager.setDDFName(table2, "flightInfo");
DDF flightTable = manager.getDDFByName("flightInfo");
flightTable.getColumnNames();
```

# ML
```

import org.apache.flink.ml.clustering.KMeans;

DDF mlData = table.VIEWS.project("ArrDelay", "DepDelay");
KMeans kmeans = (KMeans) mlData.ML.KMeans(3, 5, 5).getRawModel();

```