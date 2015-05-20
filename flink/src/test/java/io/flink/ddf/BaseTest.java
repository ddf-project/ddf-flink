package io.flink.ddf;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BaseTest {

    public static DDFManager manager;

    static Logger LOG;


    @BeforeClass
    public static void startServer() throws Exception {
        LOG = LoggerFactory.getLogger(BaseTest.class);
        manager = DDFManager.get("flink");

    }

    @AfterClass
    public static void stopServer() throws Exception {
        manager.shutdown();
    }

    public DDF createTableAirline() throws DDFException {
//        Schema schema = new Schema("airline", "Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int,TailNum string, ActualElapsedTime int, CRSElapsedTime int,AirTime int, ArrDelay int, DepDelay int, Origin string,Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,CancellationCode string, Diverted string, CarrierDelay int,WeatherDelay int, NASDelay int, SecurityDelay int, LateAircraftDelay int");
//        String airlineCsv = Thread.currentThread().getContextClassLoader().getResource("airline.csv").getPath();
//        String command = String.format("load('%s')", airlineCsv);
//        manager.sql2ddf(command, schema);
        Schema schema = new Schema("airline", "Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string, FlightNum int,TailNum string, ActualElapsedTime int, CRSElapsedTime int,AirTime int, ArrDelay int, DepDelay int, Origin string,Dest string, Distance int, TaxiIn int, TaxiOut int, Cancelled int,CancellationCode string, Diverted string, CarrierDelay string,WeatherDelay string, NASDelay string, SecurityDelay string, LateAircraftDelay string");
        List<Schema.Column> cols = schema.getColumns();
        List<String> nameAndType = Lists.transform(cols, new Function<Schema.Column, String>() {
            @Override
            public String apply(Schema.Column input) {
                String type = input.getType() == Schema.ColumnType.LOGICAL ? "bool" : input.getType().name();
                return input.getName().toLowerCase() + ":" + type.toLowerCase();
            }
        });
        String airlineCsv = Thread.currentThread().getContextClassLoader().getResource("airline.csv").getPath();
        String sql = "source(line, '" + airlineCsv + "',',',type(<" + StringUtils.join(nameAndType, ",") + ">));";
        return manager.sql2ddf(sql, schema);
    }
}
