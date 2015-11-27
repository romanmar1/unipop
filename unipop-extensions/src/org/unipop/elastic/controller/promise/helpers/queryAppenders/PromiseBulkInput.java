package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

import java.util.Map;

/**
 * Created by Karni on 11/23/2015.
 */
public class PromiseBulkInput {
    //Constructor
    public PromiseBulkInput(
            Iterable<IdPromise> bulkIdPromises,
            Iterable<TraversalPromise> bulkTraversalPromises,
            Iterable<TraversalPromise> predicatesTraversalPromises,
            Iterable<String> typesToQuery,
            SearchBuilder searchBuilder) {
        this.bulkIdPromises = bulkIdPromises;
        this.bulkTraversalPromises = bulkTraversalPromises;
        this.predicatesTraversalPromises = predicatesTraversalPromises;
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

    public Iterable<TraversalPromise> getPredicatesTraversalPromises() {
        return this.predicatesTraversalPromises;
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
    private Iterable<TraversalPromise> predicatesTraversalPromises;
    private Iterable<String> typesToQuery;

    private SearchBuilder searchBuilder;
    //endregion
}
