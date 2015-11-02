package io.ddf.flink.examples;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.analytics.Summary;
import io.ddf.etl.IHandleMissingData;
import io.ddf.exception.DDFException;
import io.ddf.flink.FlinkConstants;
import io.ddf.flink.ml.FlinkMLFacade;
import io.ddf.ml.IModel;
import io.ddf.types.AggregateTypes;
import org.apache.flink.ml.classification.SVM;
import scala.None$;
import scala.Option;

public class FlinkDDFExample {

    private static final Option None = None$.MODULE$;

    private static void runStats(DDF ddf, StringBuffer buffer) throws DDFException {

        buffer.append("\nBasic Statistics ---");
        long rowCount = ddf.getNumRows();
        buffer.append("\nrow count " + rowCount);
        buffer.append("\nColumn names " + ddf.getColumnNames());

        Summary[] s = ddf.getSummary();
        buffer.append("\nSummary is " + s[0]);

        AggregateTypes.AggregationResult aggrSum = ddf.aggregate("vs, carb, sum(mpg)");
        buffer.append("\nsum(mpg) grouped by vs and carb at(1,1) is " + aggrSum.get("1,1")[0]);

        AggregateTypes.AggregationResult aggrMean = ddf.aggregate("vs, carb, mean(mpg)");
        buffer.append("\nmean(mpg) grouped by vs and carb at(1,1) is " + aggrMean.get("1,1")[0]);

        AggregateTypes.AggregationResult aggrComb = ddf.aggregate("vs, am, sum(mpg), min(hp)");
        buffer.append("\nsum(mpg),min(hp) grouped by vs and am at(1,1) are " + aggrComb.get("1,1")[0] + " and " + aggrComb.get("1,1")[1]);

        Double[] vectorVariance = ddf.getVectorVariance("mpg");
        buffer.append("\nvector variance for mpg is " + vectorVariance[0] + "," + vectorVariance[1]);

        Double vectorMean = ddf.getVectorMean("mpg");
        buffer.append("\nmean of vector(mpg) is " + vectorMean);

        DDF newDDF = ddf.VIEWS.project("mpg", "wt");
        buffer.append("\n Projections ---");
        buffer.append("\n projected columns are " + newDDF.getColumnNames());
    }

    private static void dropNA(DDFManager manager, String filePath, StringBuffer buffer) throws DDFException {
        //using loadTable example
        buffer.append("\n Drop NA");
        DDF airlineDDF = manager.loadTable(filePath, ",");

        DDF cleanData = airlineDDF.dropNA();

        long rowCount = cleanData.getNumRows();
        buffer.append("\n row count without NA is" + rowCount);

        DDF cleanColumns = airlineDDF.dropNA(IHandleMissingData.Axis.COLUMN);
        long colCount = cleanColumns.getNumRows();
        buffer.append("\n column count when dropping columns with NA is" + colCount);

    }

    private static void runTransforms(DDF ddf, StringBuffer buffer) throws DDFException {
        DDF rescaledDDF = ddf.Transform.transformScaleMinMax();
        buffer.append("\n Transforms ---");
        buffer.append("\n rescaling using min-max " + rescaledDDF.getSummary()[0]);
    }

    private static void runAlgos(DDFManager manager, String path, StringBuffer buffer) throws DDFException {
        buffer.append("\n ALgos ---");
        manager.sql("create table iris (flower double, petal double, septal double)", FlinkConstants.ENGINE_NAME());
        manager.sql("load '" + path + "' into iris", FlinkConstants.ENGINE_NAME());
        DDF trainDDF = manager.getDDFByName("iris");
        DDF testDDF = trainDDF.VIEWS.project("petal", "septal");

        IModel imodel = ((FlinkMLFacade) trainDDF.ML).svm(None, None, None, None, None, None,None,None);

        DDF result = testDDF.ML.applyModel(imodel);
        buffer.append("\n rows in SVM result " + result.getNumRows());
    }

    public static void main(String[] args) throws DDFException {

        DDFManager manager = null;
        StringBuffer buffer = new StringBuffer();
        try {
            manager = DDFManager.get("flink");
        } catch (Exception ex) {
            System.out.println(ex);
            System.exit(-1);
        }

        String basePath = args[0];

        String filePath = basePath + "/airlineWithNA.csv";
        String algosDataPath = basePath + "/fisheriris.csv";

        //SQL2DDF
        manager.sql("CREATE TABLE mtcars (mpg double, cyl int, disp double, hp int, drat double, wt double, qesc double, vs int, am int, gear int, carb string)", FlinkConstants.ENGINE_NAME());
        manager.sql("load '" + basePath + "/mtcars' delimited by ' '  into mtcars", FlinkConstants.ENGINE_NAME());

        DDF ddf = manager.sql2ddf("select * from mtcars");

        runStats(ddf, buffer);
        dropNA(manager, filePath, buffer);
        runTransforms(ddf, buffer);
        runAlgos(manager, algosDataPath, buffer);

        System.out.println("RESULTS ARE " + buffer.toString());
    }
}
