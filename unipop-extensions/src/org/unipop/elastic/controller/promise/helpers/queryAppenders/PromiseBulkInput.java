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
            Iterable<IdPromise> idPromisesBulk,
            Iterable<TraversalPromise> traversalPromisesBulk,
            Iterable<TraversalPromise> traversalPromisesPredicates,
            Iterable<String> typesToQuery,
            SearchBuilder searchBuilder) {
        this.idPromisesBulk = idPromisesBulk;
        this.traversalPromisesBulk = traversalPromisesBulk;
        this.traversalPromisesPredicates = traversalPromisesPredicates;
        this.typesToQuery = typesToQuery;
        this.searchBuilder = searchBuilder;
    }
    //endregion

    //region Properties
    public Iterable<IdPromise> getIdPromisesBulk() {
        return this.idPromisesBulk;
    }

    public Iterable<TraversalPromise> getTraversalPromisesBulk() {
        return this.traversalPromisesBulk;
    }

    public Iterable<TraversalPromise> getTraversalPromisesPredicates() {
        return this.traversalPromisesBulk;
    }

    public Iterable<String> getTypesToQuery() {
        return this.typesToQuery;
    }

    public SearchBuilder getSearchBuilder() {
        return this.searchBuilder;
    }
    //endregion

    //region Fields
    private Iterable<IdPromise> idPromisesBulk;
    private Iterable<TraversalPromise> traversalPromisesBulk;
    private Iterable<TraversalPromise> traversalPromisesPredicates;
    private Iterable<String> typesToQuery;

    private SearchBuilder searchBuilder;
    //endregion
}
