package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/26/2015.
 */
public class IdPromiseEdgeQueryBuilderFactory implements QueryBuilderFactory<IdPromiseEdgeInput> {
    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(IdPromiseEdgeInput input) {
        List<Object> ids = StreamSupport.stream(input.getIdPromises().spliterator(), false).map(idPromise -> idPromise.getId()).collect(Collectors.toList());

        Set<String> sourceIdFields = StreamSupport.stream(input.getEdgeSchemas().spliterator(), false)
                .map(edgeSchema -> edgeSchema.getSource().get().getIdField())
                .collect(Collectors.toSet());

        if (sourceIdFields.size() == 1) {
            return new QueryBuilder().query().filtered().filter(PromiseStrings.PROMISE_SCHEMAS_ROOT).terms(sourceIdFields.iterator().next(), ids);
        } else {
            QueryBuilder queryBuilder = new QueryBuilder();
            for(String sourceIdField : sourceIdFields) {
                queryBuilder.seekRoot().query().filtered().filter(PromiseStrings.PROMISE_SCHEMAS_ROOT).bool().should().terms(sourceIdField, ids);
            }
            return queryBuilder;
        }
    }
    //endregion
}
