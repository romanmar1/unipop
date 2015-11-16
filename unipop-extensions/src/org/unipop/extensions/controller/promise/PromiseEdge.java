package org.unipop.extensions.controller.promise;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseEdge extends BaseEdge {
    //region Constructor
    public PromiseEdge(Object id, String label, Map<String, Object> keyValues, Vertex outV, Vertex inV, EdgeController controller, UniGraph graph) {
        super(id, label, keyValues, outV, inV, controller, graph);
    }
    //endregion

    //region BaseEdge Implementation
    @Override
    protected void innerAddProperty(BaseProperty vertexProperty) {

    }

    @Override
    protected void innerRemoveProperty(Property property) {

    }

    @Override
    protected void innerRemove() {

    }
    //endregion
}
