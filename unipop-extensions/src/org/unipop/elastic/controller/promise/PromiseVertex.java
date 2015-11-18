package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.BaseVertexProperty;
import org.unipop.structure.UniGraph;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseVertex extends BaseVertex<PromiseVertexController> {
    //region Constructor
    public PromiseVertex(Promise promise, UniGraph graph) {
        super(promise.getId(), "promise", null, null, graph);
        this.promise = promise;
    }
    //endregion

    //region getters & setters
    public Promise getPromise() {
        return promise;
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

    //region fields
    private Promise promise;
    //endregion
}
