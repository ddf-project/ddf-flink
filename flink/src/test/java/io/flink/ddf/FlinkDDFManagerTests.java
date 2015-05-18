package io.flink.ddf;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FlinkDDFManagerTests extends BaseTest {

    @Test
    public void testDDFConfig() throws Exception {

        Assert.assertEquals("flink", manager.getEngine());
    }

    @Test
    public void testSimpleFlinkDDFManager() throws DDFException {

        createTableAirline();

        List<String> l = manager.sql2txt("select (a) from a in airline");
        Assert.assertEquals(31, l.size());
        System.out.println(l);

        List<String> v = manager.sql2txt("count(select (a) from a in airline)");
        Assert.assertEquals(1, v.size());
        Assert.assertEquals("31", v.get(0));
        System.out.println(v);

        DDF ddf = manager.sql2ddf("select (a.year, a.month, a.dayofweek, a.deptime, a.arrtime,a.origin, a.distance, a.arrdelay, "
                + "a.depdelay, a.carrierdelay, a.weatherdelay, a.nasdelay, a.securitydelay, a.lateaircraftdelay) from a in airline");

        Assert.assertEquals(14, ddf.getSummary().length);
        System.out.println(Arrays.asList(ddf.getSummary()));
        Assert.assertEquals("ddf://adatao/" + ddf.getName(), ddf.getUri());

        manager.addDDF(ddf);
        Assert.assertEquals(ddf, manager.getDDF(ddf.getUri()));
    }
}
