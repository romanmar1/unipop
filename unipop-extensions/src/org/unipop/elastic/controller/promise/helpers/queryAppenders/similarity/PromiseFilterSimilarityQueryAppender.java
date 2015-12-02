package org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseSimilarityBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.IdPromiseSchemaInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseVertexInput;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Roman on 12/1/2015.
 */
public class PromiseFilterSimilarityQueryAppender extends PromiseSimilarityQueryAppenderBase {
    //region Constructor
    public PromiseFilterSimilarityQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> idPromiseQueryBuilderFactory,
            QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction);
        this.idPromiseQueryBuilderFactory = idPromiseQueryBuilderFactory;
        this.traversalPromiseQueryBuilderFactory = traversalPromiseQueryBuilderFactory;
    }
    //endregion

    //region PromiseSimilarityQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseSimilarityBulkInput input) {
        return Seq.seq(input.getBulkIdPromises()).isNotEmpty() ||
                Seq.seq(input.getBulkTraversalPromises()).isNotEmpty();
    }

    @Override
    public boolean append(PromiseSimilarityBulkInput input) {
        Iterable<GraphVertexSchema> vertexSchemas = Seq.seq(this.getSchemaProvider().getVertexTypes())
                .map(vertexType -> this.getSchemaProvider().getVertexSchema(vertexType))
                .filter(optional -> optional.isPresent())
                .map(optional -> optional.get())
                .toList();

        Map<String, QueryBuilder> bulkMap = new HashMap<>();

        if (Seq.seq(input.getBulkIdPromises()).isNotEmpty()) {
            QueryBuilder idPromiseQueryBuilder = this.idPromiseQueryBuilderFactory.getPromiseQueryBuilder(new IdPromiseSchemaInput(input.getBulkIdPromises(), vertexSchemas));
            bulkMap.put("idPromiseFilter", idPromiseQueryBuilder);
        }

        for(TraversalPromise traversalPromise : input.getBulkTraversalPromises()) {
            QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                    new TraversalPromiseVertexInput(
                            traversalPromise,
                            input.getSearchBuilder()));

            bulkMap.put(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        }

        addBulkPromisesToQuery(bulkMap, input.getSearchBuilder().getQueryBuilder());

        return !bulkMap.isEmpty();
    }
    //endregion

    //region Private Methods
    protected void addBulkPromisesToQuery(Map<String, QueryBuilder> bulkMap, QueryBuilder queryBuilder) {
        for (Map.Entry<String, QueryBuilder> bulkEntry : bulkMap.entrySet()) {
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
    //endregion

    //region Fields
    protected QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> idPromiseQueryBuilderFactory;
    protected QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
