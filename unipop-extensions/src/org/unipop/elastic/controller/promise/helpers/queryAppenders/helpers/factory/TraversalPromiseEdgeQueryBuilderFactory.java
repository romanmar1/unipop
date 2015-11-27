package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.TraversalEdgeRedundancyTranslator;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/26/2015.
 */
public class TraversalPromiseEdgeQueryBuilderFactory implements QueryBuilderFactory<TraversalPromiseEdgeInput> {
    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(TraversalPromiseEdgeInput input) {
        long edgeSchemasCount = StreamSupport.stream(input.getEdgeSchemas().spliterator(), false).count();

        QueryBuilder traversalPromiseQueryBuilder = edgeSchemasCount == 1 ?
                new QueryBuilder().query().filtered().filter(PromiseStringConstants.PROMISE_SCHEMAS_ROOT) :
                new QueryBuilder().query().filtered().filter().bool().should(PromiseStringConstants.PROMISE_SCHEMAS_ROOT);

        TraversalQueryTranslator traversalQueryTranslator =
                new TraversalQueryTranslator(input.getSearchBuilder(), traversalPromiseQueryBuilder);

        StreamSupport.stream(input.getEdgeSchemas().spliterator(), false).forEach(edgeSchema -> {
            try {
                // translate the traversal with redundant property names;
                TraversalPromise clonedTraversalPromise = input.getTraversalPromise().clone();

                GraphEdgeSchema.End end = input.getEdgeEnd() == TraversalPromiseEdgeInput.EdgeEnd.source ?
                        edgeSchema.getSource().get() :
                        edgeSchema.getDestination().get();
                new TraversalEdgeRedundancyTranslator(end).visit(clonedTraversalPromise.getTraversal());

                // build the query for the promise based on the promise traversal
                traversalPromiseQueryBuilder.seek(PromiseStringConstants.PROMISE_SCHEMAS_ROOT);
                traversalQueryTranslator.visit(clonedTraversalPromise.getTraversal());
            } catch (CloneNotSupportedException ex) {
                //TODO: handle clone exception
            }
        });

        return traversalPromiseQueryBuilder;
    }
    //endregion
}
