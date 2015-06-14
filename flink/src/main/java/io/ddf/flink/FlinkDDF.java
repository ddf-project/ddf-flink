package io.ddf.flink;

import io.ddf.DDF;
import io.ddf.DDFManager;
import io.ddf.content.Schema;
import io.ddf.exception.DDFException;
import io.ddf.facades.MLFacade;
import io.ddf.flink.ml.FlinkMLFacade;
import io.ddf.ml.ISupportML;

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

    @Override
    protected void initialize(DDFManager manager, Object data, Class<?>[] typeSpecs, String namespace, String name, Schema schema) throws DDFException {
        super.initialize(manager, data, typeSpecs, namespace, name, schema);
        this.ML = new FlinkMLFacade(this, this.getMLSupporter());
    }

    @Override
    public ISupportML getMLSupporter() {
        return super.getMLSupporter();
    }
}
