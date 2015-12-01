package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.IdPromiseSchemaInput;
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
            QueryBuilderFactory<IdPromiseSchemaInput<GraphEdgeSchema>> idPromiseQueryBuilderFactory,
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
        if (StreamSupport.stream(input.getBulkIdPromises().spliterator(), false).count() > 0) {
            QueryBuilder idPromiseQueryBuilder = this.idPromiseQueryBuilderFactory.getPromiseQueryBuilder(new IdPromiseSchemaInput(input.getBulkIdPromises(), edgeSchemas));
            bulkMap.put("idPromiseFilter", idPromiseQueryBuilder);
        }

        for(TraversalPromise traversalPromise : input.getBulkTraversalPromises()) {
            QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                    new TraversalPromiseEdgeInput(
                            traversalPromise,
                            input.getSearchBuilder(),
                            edgeSchemas,
                            TraversalPromiseEdgeInput.EdgeEnd.source));

            bulkMap.put(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        }

        Map<String, QueryBuilder> predicatesMap = new HashMap<>();
        if (input.getPredicatesTraversalPromises() != null &&
                StreamSupport.stream(input.getPredicatesTraversalPromises().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            for (TraversalPromise traversalPromisePredicate : input.getPredicatesTraversalPromises()) {
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

        return !bulkMap.isEmpty() || !predicatesMap.isEmpty();
    }
    //endregion

    //region Private Methods
    protected void addBulkAndPredicatesPromisesToQuery(Map<String, QueryBuilder> bulkMap, Map<String, QueryBuilder> predicatesMap, QueryBuilder queryBuilder) {

        // if there are predicates promises, the query wil have additional must nesting level
        // with should filters on bulk promises and should filters on predicate promises
        if (Seq.seq(predicatesMap.values()).count() > 0) {
            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                if (bulkMap.size() == 1) {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStringConstants.PROMISES_FILTER).must()
                            .queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
                } else {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStringConstants.PROMISES_FILTER).must()
                            .bool(PromiseStringConstants.BULK_PROMISES_FILTER).should()
                            .queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
                }
            }

            for(Map.Entry<String, QueryBuilder> predicatesEntry : predicatesMap.entrySet()) {
                if (predicatesMap.size() == 1) {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStringConstants.PROMISES_FILTER).must()
                            .queryBuilderFilter(predicatesEntry.getKey(), predicatesEntry.getValue());
                } else {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStringConstants.PROMISES_FILTER).must()
                            .bool(PromiseStringConstants.PREDICATES_PROMISES_FILTER).should()
                            .queryBuilderFilter(predicatesEntry.getKey(), predicatesEntry.getValue());
                }
            }
            // otherwise, the query will have only a should portion of the bulk promises
        } else {
            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                if (bulkMap.size() == 1) {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
                } else {
                    queryBuilder.seekRoot().query().filtered().filter()
                            .bool(PromiseStringConstants.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStringConstants.PROMISES_FILTER).should()
                            .queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
                }
            }
        }
    }
    //endregion

    //region Fields
    protected QueryBuilderFactory<IdPromiseSchemaInput<GraphEdgeSchema>> idPromiseQueryBuilderFactory;
    protected QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
