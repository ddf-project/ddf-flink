package io.ddf.flink.examples;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.Summary;
import io.ddf.exception.DDFException;
import io.ddf.types.AggregateTypes;

public class FlinkDDFExample {


    private static void runStats(DDFManager manager, DDF ddf) throws DDFException {

        long rowCount = ddf.getNumRows();
        System.out.println(rowCount);
        System.out.println(ddf.getColumnNames());

        Summary[] s = ddf.getSummary();
        System.out.println(s[0]);

        AggregateTypes.AggregationResult aggrSum = ddf.aggregate("vs, carb, sum(mpg)");
        System.out.println(aggrSum.get("1,1"));

        AggregateTypes.AggregationResult aggrMean = ddf.aggregate("vs, carb, mean(mpg)");
        System.out.println(aggrMean.get("1,1"));

        AggregateTypes.AggregationResult aggrComb = ddf.aggregate("vs, am, sum(mpg), min(hp)");
        System.out.println(aggrComb.get("1,1"));

        Double[] vectorVariance = ddf.getVectorVariance("mpg");
        System.out.println(vectorVariance);

        Double vectorMean = ddf.getVectorMean("mpg");
        System.out.println(vectorMean);

    }

    private static void dropNA(DDFManager manager) throws DDFException {
        manager.sql("create table airlineWithNA (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int,  TailNum string, ActualElapsedTime int, CRSElapsedTime int,  AirTime int, ArrDelay int, DepDelay int, Origin string,  Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,  CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )");
        manager.sql("load 'resources/test/airlineWithNA.csv' NO DEFAULTS into airlineWithNA");
        DDF airlineDDF = manager.sql2ddf("select * from airlineWithNA");
        DDF cleanData = airlineDDF.dropNA();

        long rowCount = cleanData.getNumRows();
        System.out.println(rowCount);
    }

    public static void main(String[] args) throws DDFException {

        /*manager.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string)");
        manager.sql("load 'resources/test/mtcars' delimited by ' '  into mtcars");

        DDF ddf = manager.sql2ddf("select * from mtcars");*/

        DDFManager manager = null;
            try {
                manager = DDFManager.get("flink");
            } catch (Exception ex) {
                System.out.println(ex);
                System.exit(-1);
            }

        dropNA(manager);

    }
}
