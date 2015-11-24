package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentileRanks;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

/**
 * Created by Karni on 11/23/2015.
 */
public class PromiseTypesBulkInput<TPromise> {
    //region Constructor
    public PromiseTypesBulkInput(
            Iterable<TPromise> promises,
            Iterable<TraversalPromise> traversalPromisesPredicates,
            Iterable<String> typesToQuery,
            SearchBuilder searchBuilder) {
        this.promises = promises;
        this.traversalPromisesPredicates = traversalPromisesPredicates;
        this.typesToQuery = typesToQuery;
        this.searchBuilder = searchBuilder;
    }
    //endregion

    //region Propterties
    public Iterable<TPromise> getPromises() {
        return this.promises;
    }

    public Iterable<TraversalPromise> getTraversalPromisesPredicates() {
        return this.traversalPromisesPredicates;
    }

    public Iterable<String> getTypesToQuery() {
        return this.typesToQuery;
    }

    public SearchBuilder getSearchBuilder() {
        return this.searchBuilder;
    }
    //endregion

    //region Fields
    private Iterable<TPromise> promises;
    private Iterable<TraversalPromise> traversalPromisesPredicates;
    private Iterable<String> typesToQuery;

    private SearchBuilder searchBuilder;
    //endregion
}
