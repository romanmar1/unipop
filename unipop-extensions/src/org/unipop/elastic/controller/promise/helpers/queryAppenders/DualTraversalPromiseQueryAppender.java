package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.TraversalEdgeRedundancyTranslator;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.EdgeHelper;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Karni on 11/23/2015.
 */
@SuppressWarnings("Duplicates")
public class DualTraversalPromiseQueryAppender extends GraphQueryAppenderBase<PromiseTypeBulkInput<TraversalPromise>> {
    //region Constructor
    public DualTraversalPromiseQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseTypeBulkInput<TraversalPromise> input) {
        Optional<Iterable<GraphEdgeSchema>> edgeSchemas = this.getSchemaProvider().getEdgeSchemas(input.getTypeToQuery());

        if (!edgeSchemas.isPresent() || StreamSupport.stream(edgeSchemas.get().spliterator(), false).count() == 0) {
            return false;
        }

        // making sure that all edges are dual (dual+singular edges can be supported if needed)
        if (StreamSupport.stream(edgeSchemas.get().spliterator(), false).anyMatch(edgeSchema -> {
            Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.getDirection();
            return !edgeDirection.isPresent();
        })) {
            return false;
        }

        return true;
    }

    @Override
    public boolean append(PromiseTypeBulkInput<TraversalPromise> input) {
        Optional<Iterable<GraphEdgeSchema>> edgeSchemas = this.getSchemaProvider().getEdgeSchemas(input.getTypeToQuery());

        StreamSupport.stream(input.getPromises().spliterator(), false).forEach(traversalPromise -> {
            QueryBuilder traversalPromiseQueryBuilder = new QueryBuilder();
            traversalPromiseQueryBuilder.query().filtered().filter().bool().should("promiseSchemasRoot");
            TraversalQueryTranslator traversalQueryTranslator =
                    new TraversalQueryTranslator(input.getSearchBuilder(), traversalPromiseQueryBuilder);

            StreamSupport.stream(edgeSchemas.get().spliterator(), false).forEach(edgeSchema -> {
                try {
                    // translate the traversal with redundant property names;
                    TraversalPromise clonedTraversalPromise = traversalPromise.clone();
                    new TraversalEdgeRedundancyTranslator(edgeSchema.getSource().get()).visit(clonedTraversalPromise.getTraversal());

                    // build the query for the promise based on the promise traversal
                    String promiseSchemaRoot = Integer.toString(edgeSchema.hashCode());
                    traversalPromiseQueryBuilder.seek("promiseSchemasRoot").bool(promiseSchemaRoot).must();
                    traversalQueryTranslator.visit(clonedTraversalPromise.getTraversal());

                    // finally, only if the direction is not BOTH, add a direction filter to the mix.
                    if (this.getDirection().isPresent() && this.getDirection().get() != Direction.BOTH) {
                        traversalPromiseQueryBuilder.seek("promiseSchemasRoot").bool(promiseSchemaRoot).must()
                                .term(edgeSchema.getDirection().get().getField(), getDirection().get() == Direction.IN ?
                                        edgeSchema.getDirection().get().getInValue() :
                                        edgeSchema.getDirection().get().getOutValue());
                    }

                } catch(CloneNotSupportedException ex) {
                    //TODO: handle clone exception
                }
            });

            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters("bulkPromises")
                    .filter(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        });


        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).forEach(traversalPromisePredicate -> {
                QueryBuilder traversalPromiseQueryBuilder = new QueryBuilder();
                traversalPromiseQueryBuilder.query().filtered().filter().bool().should("promiseSchemasRoot");
                TraversalQueryTranslator traversalQueryTranslator =
                        new TraversalQueryTranslator(input.getSearchBuilder(), traversalPromiseQueryBuilder);

                StreamSupport.stream(edgeSchemas.get().spliterator(), false).forEach(edgeSchema -> {
                    try {
                        // translate the traversal with redundant property names;
                        TraversalPromise clonedTraversalPromise = traversalPromisePredicate.clone();
                        new TraversalEdgeRedundancyTranslator(edgeSchema.getSource().get()).visit(clonedTraversalPromise.getTraversal());

                        // build the query for the promise based on the promise traversal
                        String promiseSchemaRoot = Integer.toString(edgeSchema.hashCode());
                        traversalPromiseQueryBuilder.seek("promiseSchemasRoot").bool(promiseSchemaRoot).must();
                        traversalQueryTranslator.visit(clonedTraversalPromise.getTraversal());
                    } catch (CloneNotSupportedException ex) {
                        //TODO: handle clone exception
                    }
                });

                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters("bulkPromises")
                        .filters("predicatesPromises")
                        .filter(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            });
        } else {
            // if we have no traversal promise predicates, and
            // we have only a single edge schema for the edge label,
            // it means we should use terms aggregation for ids.
            if (StreamSupport.stream(edgeSchemas.get().spliterator(), false).count() == 1) {
                GraphEdgeSchema edgeSchema = StreamSupport.stream(edgeSchemas.get().spliterator(), false).findFirst().get();
                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters("bulkPromises")
                        .terms(edgeSchema.getDestination().get().getIdField())
                            .field(edgeSchema.getDestination().get().getIdField())
                            .size(0)
                            .shardSize(0)
                            .executionHint("global_ordinals_low_cardinality");
            } else {
                // we have mutiple edge schema, so we must build a script aggregation to group by vertex ids
                // no matter what field is used.
                int x = 5;
            }
        }

        return true;
    }
    //endregion
}
