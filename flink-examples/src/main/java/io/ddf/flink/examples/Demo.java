package io.ddf.flink.examples;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.SqlResult;
import io.ddf.exception.DDFException;
import io.ddf.flink.FlinkConstants;
//import org.apache.flink.ml.clustering.KMeans;

import java.util.Arrays;

public class Demo {

    public static void main(String[] args) throws DDFException {
        try {
            DDFManager manager = DDFManager.get(FlinkConstants.ENGINE_NAME());
            String basePath = args[0];
            String filePath = basePath + "/airline.csv";
            manager.sql("create table airline (Year int,Month int,DayofMonth int, DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int, CRSArrTime int,UniqueCarrier string, FlightNum int,  TailNum string, ActualElapsedTime int, CRSElapsedTime int,  AirTime int, ArrDelay int, DepDelay int, Origin string,  Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,  CancellationCode string, Diverted string, CarrierDelay int, WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )", FlinkConstants.ENGINE_NAME());
            manager.sql("load '" + filePath + "' into airline", FlinkConstants.ENGINE_NAME());

            //# Table Like
            DDF table = manager.sql2ddf("select * from airline");

            table.getNumRows();
            table.getNumColumns();
            table.getColumnNames();

            DDF table2 = table.VIEWS.project("ArrDelay", "DepDelay", "Origin", "DayOfWeek", "Cancelled");
            table2.getColumnNames();

            SqlResult table3 = table2.sql("select * from @this where Origin='ISP'", "");
            table3.getRows();

            DDF table4 = table2.groupBy(Arrays.asList("Origin"), Arrays.asList("adelay=avg(ArrDelay)"));
            table4.VIEWS.top(10, "adelay", "asc");


            //# R Dataframe: xtabs, quantile, histogram
            DDF statsTable = table2.VIEWS.project("ArrDelay", "DepDelay", "DayOfWeek", "Cancelled");
            statsTable.getSummary();
            statsTable.getFiveNumSummary();
            DDF table5 = table.binning("Distance", "EQUALINTERVAL", 3, null, true, true);
            table5.getColumn("Distance").getOptionalFactor().getLevelCounts();
            DDF table6 = table2.Transform.transformScaleMinMax();

            //# Not MR
            statsTable.setMutable(true);
            statsTable.getSummary();
            statsTable.dropNA();
            statsTable.getSummary();

            // # ML
           /* DDF mlData = table.VIEWS.project("ArrDelay", "DepDelay");
            KMeans kmeans = (KMeans) mlData.ML.KMeans(3, 5, 5).getRawModel();*/

            // # Data Colab + Multi Languages
            manager.setDDFName(table2, "flightInfo");
            DDF flightTable = manager.getDDFByName("flightInfo");
            flightTable.getColumnNames();

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }
}
