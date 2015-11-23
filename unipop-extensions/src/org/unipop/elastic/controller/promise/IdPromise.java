package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Created by Karni on 11/18/2015.
 */
public class IdPromise implements Promise {
    //region Constructor
    public IdPromise(Object id, String label) {
        this.id = id;
        this.label = label;
    }
    //endregion

    //region Promise Implementation
    @Override
    public Object getId() {
        return null;
    }
    //endregion

    //region Properties
    public String getLabel() {
        return label;
    }
    //endregion

    //region fields
    private Object id;
    private String label;
    //endregion
}
