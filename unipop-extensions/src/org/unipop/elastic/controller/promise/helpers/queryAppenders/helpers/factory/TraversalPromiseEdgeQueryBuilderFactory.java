package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.TraversalEdgeRedundancyTranslator;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalIdProvider;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roman on 11/26/2015.
 */
public class TraversalPromiseEdgeQueryBuilderFactory implements QueryBuilderFactory<TraversalPromiseEdgeInput> {
    //region Constructor
    public TraversalPromiseEdgeQueryBuilderFactory(TraversalIdProvider<String> traversalIdProvider) {
        this.traversalIdProvider = traversalIdProvider;
    }
    //endregion

    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(TraversalPromiseEdgeInput input) {
        Map<String, Traversal> edgeRedundantTraversals = new HashMap<>();
        for (GraphEdgeSchema edgeSchema : input.getEdgeSchemas()) {
            try {
                // translate the traversal with redundant property names;
                TraversalPromise clonedTraversalPromise = input.getTraversalPromise().clone();

                GraphEdgeSchema.End end = input.getEdgeEnd() == TraversalPromiseEdgeInput.EdgeEnd.source ?
                        edgeSchema.getSource().get() :
                        edgeSchema.getDestination().get();
                new TraversalEdgeRedundancyTranslator(end).visit(clonedTraversalPromise.getTraversal());

                edgeRedundantTraversals.put(this.traversalIdProvider.getId(clonedTraversalPromise.getTraversal()), clonedTraversalPromise.getTraversal());

            } catch (CloneNotSupportedException ex) {
                //TODO: handle clone exception
            }
        }

        QueryBuilder traversalPromiseQueryBuilder = edgeRedundantTraversals.size() == 1 ?
                new QueryBuilder().query().filtered().filter(PromiseStrings.PROMISE_SCHEMAS_ROOT) :
                new QueryBuilder().query().filtered().filter().bool().should(PromiseStrings.PROMISE_SCHEMAS_ROOT);

        TraversalQueryTranslator traversalQueryTranslator =
                new TraversalQueryTranslator(input.getSearchBuilder(), traversalPromiseQueryBuilder);
        for(Traversal traversal : edgeRedundantTraversals.values()) {
            traversalPromiseQueryBuilder.seek(PromiseStrings.PROMISE_SCHEMAS_ROOT);
            traversalQueryTranslator.visit(traversal);
        }

        return traversalPromiseQueryBuilder;
    }

    //endregion

    //region Fields
    private TraversalIdProvider<String> traversalIdProvider;
    //endregion
}
