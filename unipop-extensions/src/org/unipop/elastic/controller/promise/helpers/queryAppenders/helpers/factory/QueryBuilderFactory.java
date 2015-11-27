package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.schema.helpers.QueryBuilder;

/**
 * Created by Roman on 11/26/2015.
 */
public interface QueryBuilderFactory<TInput> {
    QueryBuilder getPromiseQueryBuilder(TInput input);
}
