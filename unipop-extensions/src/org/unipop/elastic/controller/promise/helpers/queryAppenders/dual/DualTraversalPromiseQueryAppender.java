package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/26/2015.
 */
public class DualTraversalPromiseQueryAppender extends DualPromiseQueryAppenderBase {
    //region Constructor
    public DualTraversalPromiseQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction, traversalPromiseQueryBuilderFactory);
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllEdgeSchemasFromTypes(input.getTypesToQuery());

        Map<String, QueryBuilder> bulkMap = new HashMap<>();
        for(TraversalPromise traversalPromise : input.getTraversalPromisesBulk()) {
            QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                    new TraversalPromiseEdgeInput(
                            traversalPromise,
                            input.getSearchBuilder(),
                            edgeSchemas,
                            TraversalPromiseEdgeInput.EdgeEnd.source));

            bulkMap.put(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        }

        Map<String, QueryBuilder> predicatesMap = new HashMap<>();
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            for (TraversalPromise traversalPromisePredicate : input.getTraversalPromisesPredicates()) {
                QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                        new TraversalPromiseEdgeInput(
                                traversalPromisePredicate,
                                input.getSearchBuilder(),
                                edgeSchemas,
                                TraversalPromiseEdgeInput.EdgeEnd.destination));

                // add the promise query builder as a filter to the predicates promises filter
                predicatesMap.put(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            }
        }

        super.addBulkAndPredicatesPromisesToQuery(bulkMap, predicatesMap, input.getSearchBuilder().getQueryBuilder());

        return StreamSupport.stream(input.getTraversalPromisesBulk().spliterator(), false).count() > 0 ||
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0;
    }
    //endregion
}
