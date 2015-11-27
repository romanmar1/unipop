package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.IdPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */

//TODO: remove supression
@SuppressWarnings("Duplicates")
public class DualIdPromiseAggregationQueryAppender extends DualPromiseQueryAppenderBase  {
    //region Constructor
    public DualIdPromiseAggregationQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory,
            QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction);
        this.idPromiseQueryBuilderFactory = idPromiseQueryBuilderFactory;
        this.traversalPromiseQueryBuilderFactory = traversalPromiseQueryBuilderFactory;
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllDualEdgeSchemasFromTypes(input.getTypesToQuery());

        Set<String> sourceIdFields = StreamSupport.stream(edgeSchemas.spliterator(), false)
                .map(edgeSchema -> edgeSchema.getSource().get().getIdField())
                .collect(Collectors.toSet());

        QueryBuilder idPromiseQueryBuilder = this.idPromiseQueryBuilderFactory.getPromiseQueryBuilder(new IdPromiseEdgeInput(input.getIdPromisesBulk(), edgeSchemas));

        // aggregation layer 1
        String firstAggregationLayerName = null;
        if (sourceIdFields.size() == 1) {
            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStringConstants.BULK_ID_PROMISES)
                    // filtering relevant data to aggregate
                    .filter("IdPromiseFilter", idPromiseQueryBuilder).seek(PromiseStringConstants.BULK_ID_PROMISES)
                    // aggregate by relevant field
                    .terms(sourceIdFields.iterator().next())
                    .field(sourceIdFields.iterator().next())
                    .size(0).shardSize(0).executionHint(ExecutionHintStrings.GLOBAL_ORIDNAL_HASH);
            firstAggregationLayerName = sourceIdFields.iterator().next();
        } else {
            // If there are multiple source id fields we can't use terms aggregation as that would create inaccurate aggregation
            // instead we could build individual filters for each id, or build a special scripted terms aggregation.
            // currently unsupported...
            firstAggregationLayerName = PromiseStringConstants.BULK_ID_PROMISES;
            return false;
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


                input.getSearchBuilder().getAggregationBuilder().seek(firstAggregationLayerName)
                        .filters(PromiseStringConstants.PREDICATES_PROMISES)
                        .filter(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            }
        } else { // else - no TraversalPredicates
            Set<String> destinationIdFields = StreamSupport.stream(edgeSchemas.spliterator(), false)
                    .map(edgeSchema -> edgeSchema.getDestination().get().getIdField())
                    .collect(Collectors.toSet());

            if (destinationIdFields.size() == 1) {
                input.getSearchBuilder().getAggregationBuilder().seek(firstAggregationLayerName)
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

    //region Fields
    private QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory;
    protected QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
