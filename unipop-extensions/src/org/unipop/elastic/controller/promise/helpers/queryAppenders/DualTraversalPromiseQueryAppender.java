package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.TraversalEdgeRedundancyTranslator;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
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
@SuppressWarnings("Duplicates")
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

        // aggregation layer 1
        StreamSupport.stream(input.getPromises().spliterator(), false).forEach(traversalPromise -> {
            QueryBuilder traversalPromiseQueryBuilder = buildPromiseQuery(traversalPromise, input.getSearchBuilder(), edgeSchemas);
            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                    .filter(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        });

        // aggregation layer 2 - if TraversalPredicates exist
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).forEach(traversalPromisePredicate -> {
                QueryBuilder traversalPromiseQueryBuilder = super.buildPromisePredicateQuery(traversalPromisePredicate, input.getSearchBuilder(), edgeSchemas);
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
    private QueryBuilder buildPromiseQuery(TraversalPromise traversalPromise, SearchBuilder searchBuilder, Iterable<GraphEdgeSchema> edgeSchemas) {
        long edgeSchemasCount = StreamSupport.stream(edgeSchemas.spliterator(), false).count();

        QueryBuilder traversalPromiseQueryBuilder = edgeSchemasCount == 1 ?
                new QueryBuilder().query().filtered().filter(PromiseStringConstants.PROMISE_SCHEMAS_ROOT) :
                new QueryBuilder().query().filtered().filter().bool().should(PromiseStringConstants.PROMISE_SCHEMAS_ROOT);

        TraversalQueryTranslator traversalQueryTranslator =
                new TraversalQueryTranslator(searchBuilder, traversalPromiseQueryBuilder);

        for (GraphEdgeSchema edgeSchema : edgeSchemas) {
            try {
                traversalPromiseQueryBuilder.seek(PromiseStringConstants.PROMISE_SCHEMAS_ROOT);

                // translate the traversal with redundant property names;
                TraversalPromise clonedTraversalPromise = traversalPromise.clone();
                new TraversalEdgeRedundancyTranslator(edgeSchema.getSource().get()).visit(clonedTraversalPromise.getTraversal());

                // only if the direction is not BOTH, add a direction filter to the mix.
                if (this.getDirection().isPresent() && this.getDirection().get() != Direction.BOTH) {
                    String promiseSchemaRoot = Integer.toString(edgeSchema.hashCode());
                    traversalPromiseQueryBuilder.bool().must(promiseSchemaRoot)
                            .term(edgeSchema.getDirection().get().getField(), getDirection().get() == Direction.IN ?
                                    edgeSchema.getDirection().get().getInValue() :
                                    edgeSchema.getDirection().get().getOutValue())
                            .seek(promiseSchemaRoot);
                }

                traversalQueryTranslator.visit(clonedTraversalPromise.getTraversal());
            } catch (CloneNotSupportedException ex) {
                //TODO: handle clone exception
                int x = 5;
            }
        }

        return traversalPromiseQueryBuilder;
    }

    private Iterable<GraphEdgeSchema> getAllEdgeSchemasFromTypes(Iterable<String> edgeTypes) {
        return StreamSupport.stream(edgeTypes.spliterator(), false)
                .<GraphEdgeSchema>flatMap(typeToQuery -> this.getSchemaProvider().getEdgeSchemas(typeToQuery).isPresent() ?
                        StreamSupport.stream(this.getSchemaProvider().getEdgeSchemas(typeToQuery).get().spliterator(), false) :
                        Stream.empty())
                .collect(Collectors.toList());
    }
    //endregion

}
