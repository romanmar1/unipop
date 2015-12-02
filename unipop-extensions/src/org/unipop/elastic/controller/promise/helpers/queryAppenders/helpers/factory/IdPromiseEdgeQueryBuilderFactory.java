package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import java.util.List;
import java.util.Set;

/**
 * Created by Roman on 11/26/2015.
 */
public class IdPromiseEdgeQueryBuilderFactory implements QueryBuilderFactory<IdPromiseSchemaInput<GraphEdgeSchema>> {
    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(IdPromiseSchemaInput<GraphEdgeSchema> input) {
        List<Object> ids = Seq.seq(input.getIdPromises()).map(idPromise -> idPromise.getId()).toList();

        Set<String> sourceIdFields = Seq.seq(input.getElementSchemas()).map(elementSchema -> elementSchema.getSource().get().getIdField()).toSet();

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
