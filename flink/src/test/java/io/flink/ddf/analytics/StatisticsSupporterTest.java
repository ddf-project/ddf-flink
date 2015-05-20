package io.flink.ddf.analytics;


import io.ddf.DDF;
import io.ddf.analytics.AStatisticsSupporter.HistogramBin;
import io.ddf.exception.DDFException;
import io.flink.ddf.BaseTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class StatisticsSupporterTest extends BaseTest {
    private DDF ddf;

    @Before
    public void setUp() throws Exception {
        ddf = createTableAirline();
    }

    @Test
    public void testSummary() throws DDFException {
        Assert.assertEquals(29, ddf.getSummary().length);
        Assert.assertEquals(31, ddf.getNumRows());
    }


    //@Test TODO
    public void testSampling() throws DDFException {
        Assert.assertEquals(25, ddf.VIEWS.getRandomSample(25).size());
    }

    @Test
    public void testVectorVariance() throws DDFException {
        Double[] a = ddf.getVectorVariance("year");
        assert (a != null);
        assert (a.length == 2);
        System.out.println(">>>>> testVectorVariance = " + a[0] + "," + a[1]);
    }

    @Test
    public void testVectorMean() throws DDFException {
        Double a = ddf.getVectorMean("year");
        assert (a != null);
        System.out.println(">>>>> testVectorMean = " + a);
    }

    // @Test TODO
    public void testVectorCor() throws DDFException {
        double a = ddf.getVectorCor("year", "month");
        assert (a != Double.NaN);
        System.out.println(">>>>> testVectorCor = " + a);
    }

    //@Test TODO
    public void testVectorCovariance() throws DDFException {
        double a = ddf.getVectorCor("year", "month");
        assert (a != Double.NaN);
        System.out.println(">>>>> testVectorCovariance = " + a);
    }

    @Test
    public void testVectorQuantiles() throws DDFException {
        // Double[] quantiles = ddf1.getVectorQuantiles("deptime", {0.3, 0.5, 0.7});
        Double[] pArray = {0.3, 0.5, 0.7};
        Double[] expectedQuantiles = {801.0, 1416.0, 1644.0};
        Double[] quantiles = ddf.getVectorQuantiles("deptime", pArray);
        System.out.println("Quantiles: " + StringUtils.join(quantiles, ", "));
        Assert.assertArrayEquals(expectedQuantiles, quantiles);
    }

    @Test
    public void testVectorHistogram() throws DDFException {
        List<HistogramBin> bins = ddf.getVectorHistogram("arrdelay", 5);
        Assert.assertEquals(5, bins.size());
        //Assert.assertEquals(-12.45, bins.get(0).getX(), 0.01);
        Assert.assertEquals(-24, bins.get(0).getX(), 0.01);
        Assert.assertEquals(10, bins.get(0).getY(), 0.01);
    }


}
