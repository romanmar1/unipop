package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Created by Roman on 11/17/2015.
 */
public class Promise {
    public Promise(Object id, Traversal traversal) {
        this.id = id;
        this.traversal = traversal;
    }

    //region getters & setters
    public Object getId() {
        return id;
    }

    public Traversal getTraversal() {
        return traversal;
    }
    //endregion

    //region fields
    private Object id;
    private Traversal traversal;
    //endregion
}
