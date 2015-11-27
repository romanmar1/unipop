package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.IdPromiseEdgeInput;
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
public class DualPromiseFilterQueryAppender extends DualPromiseQueryAppenderBase {
    //region Constructor
    public DualPromiseFilterQueryAppender(
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

        Map<String, QueryBuilder> bulkMap = new HashMap<>();
        if (StreamSupport.stream(input.getIdPromisesBulk().spliterator(), false).count() > 0) {
            QueryBuilder idPromiseQueryBuilder = this.idPromiseQueryBuilderFactory.getPromiseQueryBuilder(new IdPromiseEdgeInput(input.getIdPromisesBulk(), edgeSchemas));
            bulkMap.put("idPromiseFilter", idPromiseQueryBuilder);
        }

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

        addBulkAndPredicatesPromisesToQuery(bulkMap, predicatesMap, input.getSearchBuilder().getQueryBuilder());

        return bulkMap.size() > 0 || predicatesMap.size() > 0;
    }
    //endregion

    //region Private Methods
    protected void addBulkAndPredicatesPromisesToQuery(
            Map<String, QueryBuilder> bulkMap,
            Map<String, QueryBuilder> predicatesMap,
            QueryBuilder queryBuilder) {

        // if there are predicates promises, the query wil have additional must nesting level
        // with should filters on bulk promises and should filters on predicate promises
        if (StreamSupport.stream(predicatesMap.values().spliterator(), false).count() > 0) {
            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER).must()
                    .bool(PromiseStringConstants.BULK_PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.BULK_PROMISES_FILTER)
                        .should().queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
            }

            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER).must()
                    .bool(PromiseStringConstants.PREDICATES_PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> predicatesEntry : predicatesMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.PREDICATES_PROMISES_FILTER)
                        .should().queryBuilderFilter(predicatesEntry.getKey(), predicatesEntry.getValue());
            }
            // otherwise, the query will have only a should portion of the bulk promises
        } else {
            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.PROMISES_FILTER)
                        .should().queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
            }
        }
    }
    //endregion

    //region Fields
    protected QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory;
    protected QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
