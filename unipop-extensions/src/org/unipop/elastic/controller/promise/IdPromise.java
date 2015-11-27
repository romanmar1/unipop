package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Created by Karni on 11/18/2015.
 */
public class IdPromise implements Promise {
    //region Constructor
    public IdPromise(Object id) {
        this.id = id;
    }
    //endregion

    //region Promise Implementation
    @Override
    public Object getId() {
        return id;
    }
    //endregion

    //region fields
    private Object id;
    //endregion
}
