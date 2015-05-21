package io.flink.ddf;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import org.apache.flink.api.java.DataSet;

public class FlinkDDF extends DDF {


    public FlinkDDF(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema) throws DDFException {
        super(manager, data, typeSpecs, namespace, name, schema);
    }

    public FlinkDDF(DDFManager manager, DDFManager defaultManagerIfNull) throws DDFException {
        super(manager, defaultManagerIfNull);
    }


    public FlinkDDF(DDFManager manager) throws DDFException {
        super(manager);
    }


    public DataSet<Object[]> getDataSetOfObjects() throws DDFException {
        return (DataSet<Object[]>) this.getRepresentationHandler().get(DataSet.class, Object[].class);
    }
}
