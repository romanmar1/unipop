package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

/**
 * Created by Roman on 12/1/2015.
 */
public class TraversalPromiseVertexInput extends TraversalPromiseInput {
    //region Constructor
    public TraversalPromiseVertexInput(TraversalPromise traversalPromise, SearchBuilder searchBuilder) {
        super(traversalPromise);
        this.searchBuilder = searchBuilder;
    }
    //endregion

    //region Properties
    public SearchBuilder getSearchBuilder() {
        return searchBuilder;
    }
    //endregion

    //region Fields
    private SearchBuilder searchBuilder;
    //endregion
}
