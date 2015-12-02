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
        this.setIsStrongId(true);
    }
    //endregion

    //region Override Methods
    @Override
    public TraversalPromise clone() throws CloneNotSupportedException {
        TraversalPromise clone = new TraversalPromise(this.id, this.traversal.asAdmin().clone());
        clone.setIsStrongId(this.isStrongId);
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

    public boolean getIsStrongId() {
        return this.isStrongId;
    }

    public void setIsStrongId(boolean value) {
        this.isStrongId = value;
    }
    //endregion

    //region fields
    private Object id;
    private Traversal traversal;
    private boolean isStrongId;
    //endregion
}
