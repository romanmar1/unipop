package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Karni on 11/23/2015.
 */
public class DualTraversalPromiseAggregationQueryAppender extends DualPromiseQueryAppenderBase {
    //region Constructor
    public DualTraversalPromiseAggregationQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction, traversalPromiseQueryBuilderFactory);
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllEdgeSchemasFromTypes(input.getTypesToQuery());

        // aggregation layer 1
        for(TraversalPromise traversalPromise : input.getTraversalPromisesBulk()) {
            QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                    new TraversalPromiseEdgeInput(
                        traversalPromise,
                        input.getSearchBuilder(),
                        edgeSchemas,
                        TraversalPromiseEdgeInput.EdgeEnd.source));

            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                    .filter(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        }

        // aggregation layer 2 - if TraversalPredicates exist
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            for(TraversalPromise traversalPromisePredicate : input.getTraversalPromisesPredicates()) {
                QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                        new TraversalPromiseEdgeInput(
                            traversalPromisePredicate,
                            input.getSearchBuilder(),
                            edgeSchemas,
                            TraversalPromiseEdgeInput.EdgeEnd.destination));

                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                        .filters(PromiseStringConstants.PREDICATES_PROMISES)
                        .filter(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            }

        } else { // else - no TraversalPredicates
            Set<String> destinationIdFields = StreamSupport.stream(edgeSchemas.spliterator(), false)
                    .map(edgeSchema -> edgeSchema.getDestination().get().getIdField())
                    .collect(Collectors.toSet());

            // if we have no traversal promise predicates, and we have only a single destination id field,
            // it means we should use terms aggregation for destination ids.
            if (destinationIdFields.size() == 1) {
                input.getSearchBuilder().getAggregationBuilder().seekRoot()
                        .filters(PromiseStringConstants.BULK_TRAVERSAL_PROMISES)
                        .terms(destinationIdFields.iterator().next())
                        .field(destinationIdFields.iterator().next())
                        .size(0)
                        .shardSize(0)
                        .executionHint(ExecutionHintStrings.GLOBAL_ORDINALS_LOW_CARDINALITY);
            } else {
                // If there are multiple destination id fields we can't use terms aggregation as that would create inaccurate aggregation
                // instead we could build a special scripted terms aggregation.
                // currently unsupported...
                return false;
            }
        }

        return true;
    }
    //endregion

}
