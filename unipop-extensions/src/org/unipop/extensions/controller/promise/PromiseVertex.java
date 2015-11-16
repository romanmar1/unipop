package org.unipop.extensions.controller.promise;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseVertex extends BaseVertex<PromiseVertexController> {
    //region Constructor
    protected PromiseVertex(Object id, String label, Map<String, Object> keyValues, PromiseVertexController controller, UniGraph graph) {
        super(id, label, keyValues, controller, graph);
    }
    //endregion

    //region BaseVertex Implementation
    @Override
    protected void innerAddProperty(BaseVertexProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
    //endregion
}
