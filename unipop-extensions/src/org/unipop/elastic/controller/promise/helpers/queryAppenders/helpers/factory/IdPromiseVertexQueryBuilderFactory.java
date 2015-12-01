package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 12/1/2015.
 */
public class IdPromiseVertexQueryBuilderFactory implements QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> {
    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(IdPromiseSchemaInput<GraphVertexSchema> input) {
        List<String> ids = Seq.seq(input.getIdPromises()).map(idPromise -> idPromise.getId().toString()).toList();
        String[] types = Seq.seq(input.getElementSchemas()).map(vertexSchema -> vertexSchema.getType()).toArray(String[]::new);

        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.seekRoot().query().filtered().filter().ids(ids, types);
        return queryBuilder;
    }
    //endregion
}
