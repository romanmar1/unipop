package org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseSimilarityBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseVertexInput;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseVertexSchema;
import org.unipop.elastic.controller.schema.helpers.ExecutionHintStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 12/2/2015.
 */
public class TraversalPromiseSimilarityQueryAppender extends PromiseSimilarityQueryAppenderBase{
    //region Constructor
    public TraversalPromiseSimilarityQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction);
        this.traversalPromiseQueryBuilderFactory = traversalPromiseQueryBuilderFactory;
    }
    //endregion

    //region PromiseSimilarityQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseSimilarityBulkInput input) {
        return this.getGraphPromiseVertexSchema().isPresent() && Seq.seq(input.getBulkTraversalPromises()).isNotEmpty();
    }

    @Override
    public boolean append(PromiseSimilarityBulkInput input) {
        Optional<GraphPromiseVertexSchema> graphPromiseVertexSchema = this.getGraphPromiseVertexSchema();
        if (graphPromiseVertexSchema.get().getSimilarity() == null ||
                graphPromiseVertexSchema.get().getSimilarity().getSimilarityProperties() == null ||
                Seq.seq(graphPromiseVertexSchema.get().getSimilarity().getSimilarityProperties()).isEmpty()) {
            return false;
        }

        // aggregation layer 1
        for(TraversalPromise traversalPromise : input.getBulkTraversalPromises()) {
            QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                    new TraversalPromiseVertexInput(
                            traversalPromise,
                            input.getSearchBuilder()));

            input.getSearchBuilder().getAggregationBuilder().seekRoot().filters(PromiseStrings.BULK_TRAVERSAL_PROMISES)
                    .filter(traversalPromise.getId().toString(), traversalPromiseQueryBuilder);
        }

        for(String similarityProperty : Seq.seq(graphPromiseVertexSchema.get().getSimilarity().getSimilarityProperties()).toSet()) {
            input.getSearchBuilder().getAggregationBuilder().seekRoot()
                    .filters(PromiseStrings.BULK_TRAVERSAL_PROMISES)
                    .terms(similarityProperty)
                    .field(similarityProperty)
                    .size(0).shardSize(0).executionHint(ExecutionHintStrings.GLOBAL_ORDINALS_LOW_CARDINALITY);
        }

        return true;
    }
    //endregion

    //region Fields
    private QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory;
    //endregion
}
