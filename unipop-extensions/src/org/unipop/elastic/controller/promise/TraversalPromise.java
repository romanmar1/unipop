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
        TraversalPromise clone = (TraversalPromise)super.clone();
        clone.id = this.id;
        clone.traversal = this.traversal.asAdmin().clone();
        return clone;
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
