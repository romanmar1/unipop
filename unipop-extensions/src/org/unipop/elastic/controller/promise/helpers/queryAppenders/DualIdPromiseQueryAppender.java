package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.EdgeHelper;
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
public class DualIdPromiseQueryAppender extends DualPromiseQueryAppenderBase<PromiseTypesBulkInput<IdPromise>>  {
    //region Constructor
    public DualIdPromiseQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseTypesBulkInput<IdPromise> input) {
        Iterator<IdPromise> promises = input.getPromises().iterator();
        if (!promises.hasNext()) {
            return false;
        }
        IdPromise firstIdPromise = input.getPromises().iterator().next();

        String typeToQuery = StreamSupport.stream(input.getTypesToQuery().spliterator(), false).findFirst().get();
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(typeToQuery, Optional.of(firstIdPromise.getLabel()), null);

        if (!edgeSchema.isPresent()) {
            return false;
        }

        Optional<GraphEdgeSchema.Direction> edgeDirection = edgeSchema.get().getDirection();
        if (!edgeDirection.isPresent()) {
            return false;
        }

        String sourceType = EdgeHelper.getEdgeSourceType(edgeSchema.get(), null);
        return sourceType != null && sourceType.equals(firstIdPromise.getLabel());
    }

    @Override
    public boolean append(PromiseTypesBulkInput<IdPromise> input) {
        // We assume a greater entity made sure that all promises are the same type, so we take the 1st one and use it to get the schema
        Iterator<IdPromise> promises = input.getPromises().iterator();
        if (!promises.hasNext()) {
            return false;
        }

        // We assume a single type to query is given to this appender inspite of its ability to recieve more
        String typeToQuery = StreamSupport.stream(input.getTypesToQuery().spliterator(), false).findFirst().get();

        IdPromise firstIdPromise = input.getPromises().iterator().next();
        Optional<GraphEdgeSchema> edgeSchema = this.getSchemaProvider().getEdgeSchema(typeToQuery, Optional.of(firstIdPromise.getLabel()), null);
        if (!edgeSchema.isPresent()) {
            return false;
        }

        // aggregation layer 1
        QueryBuilder idPromiseQueryBuilder = buildPromiseQueryFilter(input.getPromises(), edgeSchema.get());
        input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStringConstants.BULK_ID_PROMISES)
                // filtering relevant data to aggregate
                .filter(firstIdPromise.getLabel(), idPromiseQueryBuilder).seek(PromiseStringConstants.BULK_ID_PROMISES)
                // aggregate by relevant field
                .terms(edgeSchema.get().getSource().get().getIdField())
                .field(edgeSchema.get().getSource().get().getIdField())
                .size(0).shardSize(0).executionHint(ExecutionHintStrings.GLOBAL_ORIDNAL_HASH);


        // aggregation layer 2 - if TraversalPredicates exist
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).forEach(traversalPromisePredicate -> {
                QueryBuilder traversalPromiseQueryBuilder = super.buildPromisePredicateQuery(
                        traversalPromisePredicate,
                        input.getSearchBuilder(),
                        Arrays.asList(edgeSchema.get()));
                input.getSearchBuilder().getAggregationBuilder().seek(edgeSchema.get().getSource().get().getIdField())
                        .filters(PromiseStringConstants.PREDICATES_PROMISES)
                        .filter(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            });
        } else { // else - no TraversalPredicates
            input.getSearchBuilder().getAggregationBuilder().seek(edgeSchema.get().getSource().get().getIdField())
                    .terms(edgeSchema.get().getDestination().get().getIdField())
                    .field(edgeSchema.get().getDestination().get().getIdField())
                    .size(0)
                    .shardSize(0)
                    .executionHint(ExecutionHintStrings.GLOBAL_ORDINALS_LOW_CARDINALITY);
        }

        return true;
    }
    //endregion

    //region Private Methods
    private QueryBuilder buildPromiseQueryFilter(Iterable<IdPromise> idPromises, GraphEdgeSchema edgeSchema) {
        QueryBuilder idPromiseQueryBuilder = new QueryBuilder().query().filtered().filter(PromiseStringConstants.PROMISE_SCHEMAS_ROOT);

        // only if the direction is not BOTH, add a direction filter to the mix.
        if (this.getDirection().isPresent() && this.getDirection().get() != Direction.BOTH) {
            String promiseSchemaRoot = Integer.toString(edgeSchema.hashCode());
            idPromiseQueryBuilder.bool().must(promiseSchemaRoot)
                    .term(edgeSchema.getDirection().get().getField(), getDirection().get() == Direction.IN ?
                            edgeSchema.getDirection().get().getInValue() :
                            edgeSchema.getDirection().get().getOutValue())
                    .seek(promiseSchemaRoot);
        }

        idPromiseQueryBuilder.terms(
                edgeSchema.getSource().get().getIdField(),
                StreamSupport.stream(idPromises.spliterator(), false).map(idPromise -> idPromise.getId()).collect(Collectors.toList()));

        return idPromiseQueryBuilder;
    }
    //endregion
}
