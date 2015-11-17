package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseProperty;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseEdge extends BaseEdge {
    //region Constructor
    public PromiseEdge(Object id, Vertex outV, Vertex inV, UniGraph graph) {
        super(id, "promise", null, outV, inV, null, graph);
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
