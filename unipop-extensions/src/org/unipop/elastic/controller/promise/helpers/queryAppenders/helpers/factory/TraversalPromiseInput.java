package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.TraversalPromise;

/**
 * Created by Roman on 11/26/2015.
 */
public class TraversalPromiseInput {
    //region Constructor
    public TraversalPromiseInput(TraversalPromise traversalPromise) {
        this.traversalPromise = traversalPromise;
    }
    //endregion

    //region Properties
    public TraversalPromise getTraversalPromise() {
        return traversalPromise;
    }
    //endregion

    //region Fields
    private TraversalPromise traversalPromise;
    //endregion
}
