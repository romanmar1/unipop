package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Created by Karni on 11/18/2015.
 */
public class TraversalPromise implements Promise{
    //region Constructor
    public TraversalPromise(Object id, Traversal traversal) {
        this.id = id;
        this.traversal = traversal;
    }
    //endregion

    //region Override Methods
    @Override
    public TraversalPromise clone() throws CloneNotSupportedException {
        return new TraversalPromise(this.id, this.traversal.asAdmin().clone());
    }
    //endregion

    //region Promise Implementation
    public Object getId() {
        return id;
    }
    //endregion

    //region getters & setters
    public Traversal getTraversal() {
        return traversal;
    }
    //endregion

    //region fields
    private Object id;
    private Traversal traversal;
    //endregion
}
