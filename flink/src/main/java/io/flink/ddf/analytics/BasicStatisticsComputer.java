package io.flink.ddf.analytics;


import io.ddf.DDF;
import io.ddf.analytics.AStatisticsSupporter;
import io.ddf.analytics.Summary;
import io.ddf.exception.DDFException;
import io.flink.ddf.FlinkDDFManager;
import io.flink.ddf.utils.Utils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Compute the basic statistics for each column in a DataSet-based DDF
 */
public class BasicStatisticsComputer extends AStatisticsSupporter {

    public BasicStatisticsComputer(DDF theDDF) {
        super(theDDF);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Summary[] getSummaryImpl() throws DDFException {
        FlinkDDFManager manager = (FlinkDDFManager) this.getManager();
        DataSet<Object[]> data = (DataSet<Object[]>) this.getDDF().getRepresentationHandler().get(DataSet.class, Object[].class);
        ReduceOperator<Summary[]> statsDataSet = data.map(new GetSummaryMapper()).reduce(new GetSummaryReducer());
        List<Summary[]> stats;
        try {
            stats = Utils.collect(manager.getExecutionEnvironment(), statsDataSet);
        } catch (Exception e) {
            throw new DDFException(e);
        }
        if (stats != null && !stats.isEmpty()) return stats.get(0);
        return null;
    }

    /**
     * Mapper function to accumulate summary data from each row
     */
    @SuppressWarnings("serial")
    public static class GetSummaryMapper implements MapFunction<Object[], Summary[]> {
        private final Logger mLog = LoggerFactory.getLogger(this.getClass());

        @Override
        public Summary[] map(Object[] p) {
            int dim = p.length;
            if (p != null && dim > 0) {
                Summary[] result = new Summary[dim];
                for (int i = 0; i < dim; i++) {
                    Summary s = new Summary();
                    if (null == p[i]) {
                        result[i] = null;
                    } else {
                        if (p[i] instanceof Double) {
                            Double a = (Double) p[i];
                            result[i] = s.merge(a);
                        } else if (p[i] instanceof Integer) {
                            Double a = Double.parseDouble(p[i].toString());
                            result[i] = s.merge(a);
                        } else if (p[i] != null) {
                            String str = p[i].toString();
                            if (str.trim().equalsIgnoreCase("NA")) {
                                result[i] = new Summary();
                                result[i].setNACount(1);
                            } else {
                                double number = 0.0;

                                if (NumberUtils.isNumber(str)) {
                                    number = Double.parseDouble(str);
                                    s.merge(number);
                                } else {
                                    result[i] = null;
                                }
                            }
                        }
                    }
                }
                return result;
            } else {
                mLog.error("malformed line input");
                return null;
            }
        }
    }


    @SuppressWarnings("serial")
    public static class GetSummaryReducer implements ReduceFunction<Summary[]> {
        @Override
        public Summary[] reduce(Summary[] a, Summary[] b) {
            int dim = a.length;
            Summary[] result = new Summary[dim];

            for (int i = 0; i < dim; i++) {
                // normal cases
                if (a[i] != null && b[i] != null) {
                    if (!a[i].isNA() && !b[i].isNA()) {
                        result[i] = a[i].merge(b[i]);
                    } else if (!a[i].isNA()) {
                        result[i] = a[i];
                        result[i].addToNACount(b[i].NACount());
                    } else if (!b[i].isNA()) {
                        result[i] = b[i];
                        result[i].addToNACount(a[i].NACount());
                    }
                    // both are NAs
                    else {
                        result[i] = new Summary();
                        result[i].setNACount(a[i].NACount() + b[i].NACount());
                    }
                } else {
                    if (a[i] != null) {
                        result[i] = new Summary();
                        result[i] = a[i];
                        result[i].addToNACount(1);
                    } else if (b[i] != null) {
                        result[i] = new Summary();
                        result[i] = b[i];
                        result[i].addToNACount(1);
                    }
                }
            }
            return result;
        }
    }
}
