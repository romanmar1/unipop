package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.Promise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.TraversalEdgeRedundancyTranslator;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */
public abstract class DualPromiseQueryAppenderBase<TInput extends PromiseTypesBulkInput<? extends Promise>> extends GraphQueryAppenderBase<TInput> {
    //region Constructor
    public DualPromiseQueryAppenderBase(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region Protected Methods
    protected QueryBuilder buildPromisePredicateQuery(TraversalPromise traversalPromisePredicate, SearchBuilder searchBuilder, Iterable<GraphEdgeSchema> edgeSchemas) {
        long edgeSchemasCount = StreamSupport.stream(edgeSchemas.spliterator(), false).count();

        QueryBuilder traversalPromiseQueryBuilder = edgeSchemasCount == 1 ?
                new QueryBuilder().query().filtered().filter("promiseSchemasRoot") :
                new QueryBuilder().query().filtered().filter().bool().should("promiseSchemasRoot");

        TraversalQueryTranslator traversalQueryTranslator =
                new TraversalQueryTranslator(searchBuilder, traversalPromiseQueryBuilder);

        StreamSupport.stream(edgeSchemas.spliterator(), false).forEach(edgeSchema -> {
            try {
                // translate the traversal with redundant property names;
                TraversalPromise clonedTraversalPromise = traversalPromisePredicate.clone();
                new TraversalEdgeRedundancyTranslator(edgeSchema.getDestination().get()).visit(clonedTraversalPromise.getTraversal());

                // build the query for the promise based on the promise traversal
                traversalQueryTranslator.visit(clonedTraversalPromise.getTraversal());
            } catch (CloneNotSupportedException ex) {
                //TODO: handle clone exception
            }
        });

        return traversalPromiseQueryBuilder;
    }
    //endregion
}
