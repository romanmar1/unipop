package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.schema.helpers.QueryBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 11/26/2015.
 */
public class CachedTraversalPromiseEdgeQueryBuilderFactory<TInput extends TraversalPromiseEdgeInput> implements QueryBuilderFactory<TInput> {
    //region Constructor
    public CachedTraversalPromiseEdgeQueryBuilderFactory(QueryBuilderFactory<TInput> innerFactory) {
        this.innerFactory = innerFactory;
        this.cache = new HashMap<>();
    }
    //endregion

    //region QueryBuilderFactory
    @Override
    public QueryBuilder getPromiseQueryBuilder(TInput input) {
        String key = input.getTraversalPromise().getId().toString() + "_" + input.getEdgeEnd().toString();
        QueryBuilder cachedQueryBuilder = this.cache.get(key);
        if (cachedQueryBuilder == null) {
            cachedQueryBuilder = this.innerFactory.getPromiseQueryBuilder(input);
            this.cache.put(key, cachedQueryBuilder);
        }

        return cachedQueryBuilder;
    }
    //endregion

    //region Fields
    private QueryBuilderFactory<TInput> innerFactory;
    private Map<Object, QueryBuilder> cache;
    //endregion
}
