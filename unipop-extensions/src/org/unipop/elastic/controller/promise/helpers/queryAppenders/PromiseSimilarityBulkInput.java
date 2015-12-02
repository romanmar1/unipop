package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

/**
 * Created by Roman on 12/1/2015.
 */
public class PromiseSimilarityBulkInput {
    //Constructor
    public PromiseSimilarityBulkInput(
            Iterable<IdPromise> bulkIdPromises,
            Iterable<TraversalPromise> bulkTraversalPromises,
            Iterable<String> typesToQuery,
            SearchBuilder searchBuilder) {
        this.bulkIdPromises = bulkIdPromises;
        this.bulkTraversalPromises = bulkTraversalPromises;
        this.typesToQuery = typesToQuery;
        this.searchBuilder = searchBuilder;
    }
    //endregion

    //region Properties
    public Iterable<IdPromise> getBulkIdPromises() {
        return this.bulkIdPromises;
    }

    public Iterable<TraversalPromise> getBulkTraversalPromises() {
        return this.bulkTraversalPromises;
    }

    public Iterable<String> getTypesToQuery() {
        return this.typesToQuery;
    }

    public SearchBuilder getSearchBuilder() {
        return this.searchBuilder;
    }
    //endregion

    //region Fields
    private Iterable<IdPromise> bulkIdPromises;
    private Iterable<TraversalPromise> bulkTraversalPromises;
    private Iterable<String> typesToQuery;
    private SearchBuilder searchBuilder;
    //endregion
}
