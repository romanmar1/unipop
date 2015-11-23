package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentileRanks;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

/**
 * Created by Karni on 11/23/2015.
 */
public class PromiseTypeBulkInput<TPromise> {
    //region Constructor
    public PromiseTypeBulkInput(
            Iterable<TPromise> promises,
            Iterable<TraversalPromise> traversalPromisesPredicates,
            String typeToQuery,
            SearchBuilder searchBuilder) {
        this.promises = promises;
        this.traversalPromisesPredicates = traversalPromisesPredicates;
        this.typeToQuery = typeToQuery;
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

    public String getTypeToQuery() {
        return this.typeToQuery;
    }

    public SearchBuilder getSearchBuilder() {
        return this.searchBuilder;
    }
    //endregion

    //region Fields
    private Iterable<TPromise> promises;
    private Iterable<TraversalPromise> traversalPromisesPredicates;
    private String typeToQuery;

    private SearchBuilder searchBuilder;
    //endregion
}
