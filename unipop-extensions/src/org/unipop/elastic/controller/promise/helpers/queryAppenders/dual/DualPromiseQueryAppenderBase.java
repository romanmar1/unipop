package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */
public abstract class DualPromiseQueryAppenderBase extends GraphQueryAppenderBase<PromiseBulkInput> {
    //region Constructor
    public DualPromiseQueryAppenderBase(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction);

        this.traversalPromiseQueryBuilderFactory = traversalPromiseQueryBuilderFactory;
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseBulkInput input) {
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
    //endregion

    //region Protected Methods
    protected Iterable<GraphEdgeSchema> getAllEdgeSchemasFromTypes(Iterable<String> edgeTypes) {
        return StreamSupport.stream(edgeTypes.spliterator(), false)
                .<GraphEdgeSchema>flatMap(typeToQuery -> this.getSchemaProvider().getEdgeSchemas(typeToQuery).isPresent() ?
                        StreamSupport.stream(this.getSchemaProvider().getEdgeSchemas(typeToQuery).get().spliterator(), false) :
                        Stream.empty())
                .collect(Collectors.toList());
    }

    protected void addBulkAndPredicatesPromisesToQuery(
            Map<String, QueryBuilder> bulkMap,
            Map<String, QueryBuilder> predicatesMap,
            QueryBuilder queryBuilder) {

        // if there are predicates promises, the query wil have additional must nesting level
        // with should filters on bulk promises and should filters on predicate promises
        if (StreamSupport.stream(predicatesMap.values().spliterator(), false).count() > 0) {
            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_AND_TYPES_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER).must()
                    .bool(PromiseStringConstants.BULK_PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.BULK_PROMISES_FILTER)
                        .should().queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
            }

            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_AND_TYPES_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER).must()
                    .bool(PromiseStringConstants.PREDICATES_PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> predicatesEntry : predicatesMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.PREDICATES_PROMISES_FILTER)
                        .should().queryBuilderFilter(predicatesEntry.getKey(), predicatesEntry.getValue());
            }
        // otherwise, the query will have only a should portion of the bulk promises
        } else {
            queryBuilder.seekRoot().query().filtered().filter()
                    .bool(PromiseStringConstants.PROMISES_AND_TYPES_FILTER).must()
                    .bool(PromiseStringConstants.PROMISES_FILTER);

            for(Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
                queryBuilder.seek(PromiseStringConstants.PROMISES_FILTER)
                        .should().queryBuilderFilter(bulkEntry.getKey(), bulkEntry.getValue());
            }
        }
    }
    //endregion

    //region Fields
    protected QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
