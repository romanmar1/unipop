package org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseSimilarityBulkInput;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 12/1/2015.
 */
public class PromiseTypesSimilarityQueryAppender extends PromiseSimilarityQueryAppenderBase {
    //region Constructor
    public PromiseTypesSimilarityQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region PromiseSimilarityQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseSimilarityBulkInput input) {
        return Seq.seq(this.getSchemaProvider().getVertexTypes()).isNotEmpty();
    }

    @Override
    public boolean append(PromiseSimilarityBulkInput input) {
        if (canAppend(input)) {
            input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                    .bool(PromiseStrings.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                    .terms(PromiseStrings.TYPES_FILTER, "_type", this.getSchemaProvider().getVertexTypes());

            return true;
        }

        return false;
    }
    //endregion
}
