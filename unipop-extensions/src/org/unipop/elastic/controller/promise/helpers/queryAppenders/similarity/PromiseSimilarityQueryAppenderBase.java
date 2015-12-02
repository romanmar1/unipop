package org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseSimilarityBulkInput;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseVertexSchema;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 12/1/2015.
 */
public abstract class PromiseSimilarityQueryAppenderBase extends GraphQueryAppenderBase<PromiseSimilarityBulkInput> {
    //region Constructor
    public PromiseSimilarityQueryAppenderBase(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region Protected Methods
    protected Optional<GraphPromiseVertexSchema> getGraphPromiseVertexSchema() {
        Optional<GraphVertexSchema> vertexSchema = this.getSchemaProvider().getVertexSchema("promise");
        if (vertexSchema.isPresent() && GraphPromiseVertexSchema.class.isAssignableFrom(vertexSchema.get().getClass())) {
            return Optional.of((GraphPromiseVertexSchema)vertexSchema.get());
        }

        return Optional.empty();
    }
    //endregion
}
