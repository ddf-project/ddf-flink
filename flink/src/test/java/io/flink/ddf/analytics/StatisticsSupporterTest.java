package io.flink.ddf.analytics;


import io.ddf.DDF;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;
import io.ddf.exception.DDFException;
import io.flink.ddf.BaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class StatisticsSupporterTest extends BaseTest {
    private DDF ddf, ddf1;

    @Before
    public void setUp() throws Exception {
        createTableAirline();

        ddf = manager.sql2ddf("select (a.year, a.month, a.dayofweek, a.deptime, a.arrtime,a.origin, a.distance, a.arrdelay, "
                + "a.depdelay, a.carrierdelay, a.weatherdelay, a.nasdelay, a.securitydelay, a.lateaircraftdelay) from a in airline");
        ddf1 = manager.sql2ddf("select (a.year, a.month, a.dayofweek, a.deptime, a.arrdelay) from a in airline");
    }

    @Test
    public void testSummary() throws DDFException {
        Assert.assertEquals(14, ddf.getSummary().length);
        //TODO
        //Assert.assertEquals(31, ddf.getNumRows());
    }


    //@Test TODO
    public void testSampling() throws DDFException {
        DDF ddf2 = manager.sql2ddf("select (a) from a in airline");
        Assert.assertEquals(25, ddf2.VIEWS.getRandomSample(25).size());
    }

    @Test
    public void testVectorVariance() throws DDFException {
        DDF ddf2 = manager.sql2ddf("select (a) from a in airline");
        Double[] a = ddf2.getVectorVariance("year");
        assert (a != null);
        assert (a.length == 2);
    }

    @Test
    public void testVectorMean() throws DDFException {
        DDF ddf2 = manager.sql2ddf("select (a) from a in airline");
        Double a = ddf2.getVectorMean("year");
        assert (a != null);
        System.out.println(">>>>> testVectorMean = " + a);
    }

    // @Test
    public void testVectorCor() throws DDFException {
        double a = ddf1.getVectorCor("year", "month");
        assert (a != Double.NaN);
        System.out.println(">>>>> testVectorCor = " + a);
    }

    //@Test
    public void testVectorCovariance() throws DDFException {
        double a = ddf1.getVectorCor("year", "month");
        assert (a != Double.NaN);
        System.out.println(">>>>> testVectorCovariance = " + a);
    }

//  @Test
//  public void testVectorQuantiles() throws DDFException {
//    // Double[] quantiles = ddf1.getVectorQuantiles("deptime", {0.3, 0.5, 0.7});
//    Double[] pArray = { 0.3, 0.5, 0.7 };
//    Double[] expectedQuantiles = { 801.0, 1416.0, 1644.0 };
//    Double[] quantiles = ddf1.getVectorQuantiles("deptime", pArray);
//    System.out.println("Quantiles: " + StringUtils.join(quantiles, ", "));
//    Assert.assertArrayEquals(expectedQuantiles, quantiles);
//  }

    @Test
    public void testVectorHistogram() throws DDFException {
        List<HistogramBin> bins = ddf1.getVectorHistogram("arrdelay", 5);
        Assert.assertEquals(5, bins.size());
        Assert.assertEquals(-12.45, bins.get(0).getX(), 0.01);
        Assert.assertEquals(11, bins.get(0).getY(), 0.01);
    }


}
