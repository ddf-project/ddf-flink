package io.ddf.flink.examples;


import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.exception.DDFException;

public class RowCount {

  public static void main(String[] args) throws DDFException {

    DDFManager manager = DDFManager.get("flink");

    manager.sql2txt("create table airline (Year int,Month int,DayofMonth int,"
        + "DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,"
        + "CRSArrTime int,UniqueCarrier string, FlightNum int, "
        + "TailNum string, ActualElapsedTime int, CRSElapsedTime int, "
        + "AirTime int, ArrDelay int, DepDelay int, Origin string, "
        + "Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int, "
        + "CancellationCode string, Diverted string, CarrierDelay int, "
        + "WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int )");

    manager.sql2txt("load 'resources/test/airline.csv' into airline");

    DDF ddf = manager.sql2ddf("SELECT * FROM airline");

    long nrow = ddf.getNumRows();
    int ncol = ddf.getNumColumns();

    System.out.println("Number of data row is " + nrow);
    System.out.println("Number of data columns is " + ncol);

  }
}
