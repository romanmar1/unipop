package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Karni on 11/23/2015.
 */
public class DualTraversalPromiseQueryAppender extends DualPromiseQueryAppenderBase<PromiseTypesBulkInput<TraversalPromise>> {
    //region Constructor
    public DualTraversalPromiseQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseTypesBulkInput<TraversalPromise> input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllEdgeSchemasFromTypes(input.getTypesToQuery());

        if (StreamSupport.stream(edgeSchemas.spliterator(), false).count() == 0) {
            return false;
        }

        // making sure that all edges are dual (dual+singular edges can be supported if needed)
        if (StreamSupport.stream(edgeSchemas.spliterator(), false).anyMatch(edgeSchema -> {
            Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.getDirection();
            return !edgeDirection.isPresent();
        })) {
            return false;
        }

        return true;
    }

    @Override
    public boolean append(PromiseTypesBulkInput<TraversalPromise> input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllEdgeSchemasFromTypes(input.getTypesToQuery());

        if (StreamSupport.stream(input.getPromises().spliterator(), false).count() == 0) {
            return false;
        }

        StreamSupport.stream(input.getPromises().spliterator(), false).forEach(traversalPromise -> {
            QueryBuilder traversalPromiseQueryBuilder = super.buildPromiseQuery(
                    traversalPromise,
                    input.getSearchBuilder(),
                    edgeSchemas,
                    edgeSchema -> edgeSchema.getSource().get());

            // Add the promise query builder as a filter to the query
            input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                    .bool().must(PromiseStringConstants.PROMISES_AND_TYPES_FILTER)
                    .bool(PromiseStringConstants.PROMISES_FILTER).should().queryBuilderFilter(traversalPromiseQueryBuilder);

            // aggregation layer 1
            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                    .filter(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        });

        // aggregation layer 2 - if TraversalPredicates exist
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).forEach(traversalPromisePredicate -> {
                QueryBuilder traversalPromiseQueryBuilder = super.buildPromiseQuery(
                        traversalPromisePredicate,
                        input.getSearchBuilder(),
                        edgeSchemas,
                        edgeSchema -> edgeSchema.getDestination().get());

                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                        .filters(PromiseStringConstants.PREDICATES_PROMISES)
                        .filter(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            });
        } else { // else - no TraversalPredicates
            // if we have no traversal promise predicates, and
            // we have only a single edge schema for the edge label,
            // it means we should use terms aggregation for ids.
            if (StreamSupport.stream(edgeSchemas.spliterator(), false).count() == 1) {
                GraphEdgeSchema edgeSchema = StreamSupport.stream(edgeSchemas.spliterator(), false).findFirst().get();
                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                        .terms(edgeSchema.getDestination().get().getIdField())
                        .field(edgeSchema.getDestination().get().getIdField())
                        .size(0)
                        .shardSize(0)
                        .executionHint(ExecutionHintStrings.GLOBAL_ORDINALS_LOW_CARDINALITY);
            } else {
                // we have multiple edge schema, so we must build a script aggregation to group by vertex ids
                // no matter what field is used.
                int x = 5;
            }
        }

        return true;
    }
    //endregion

    //region Private Methods
    private Iterable<GraphEdgeSchema> getAllEdgeSchemasFromTypes(Iterable<String> edgeTypes) {
        return StreamSupport.stream(edgeTypes.spliterator(), false)
                .<GraphEdgeSchema>flatMap(typeToQuery -> this.getSchemaProvider().getEdgeSchemas(typeToQuery).isPresent() ?
                        StreamSupport.stream(this.getSchemaProvider().getEdgeSchemas(typeToQuery).get().spliterator(), false) :
                        Stream.empty())
                .collect(Collectors.toList());
    }
    //endregion

}
