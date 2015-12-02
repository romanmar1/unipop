package org.unipop.elastic.controller.promise.helpers.elementConverters.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalIdProvider;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseVertexSchema;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 12/2/2015.
 */
public abstract class GraphPromiseSimilarityEdgeConverterBase<TElementSource> implements ElementConverter<TElementSource, Element> {
    //region Constructor
    public GraphPromiseSimilarityEdgeConverterBase(
            UniGraph graph,
            Direction direction,
            GraphElementSchemaProvider schemaProvider,
            TraversalIdProvider<String> traversalIdProvider) {
        this.graph = graph;
        this.direction = direction;
        this.schemaProvider = schemaProvider;
        this.traversalIdProvider = traversalIdProvider;
    }
    //endregion

    //region Protected Methods
    protected Optional<GraphPromiseVertexSchema> getGraphPromiseVertexSchema() {
        Optional<GraphVertexSchema> vertexSchema = this.schemaProvider.getVertexSchema("promise");
        if (vertexSchema.isPresent() && GraphPromiseVertexSchema.class.isAssignableFrom(vertexSchema.get().getClass())) {
            return Optional.of((GraphPromiseVertexSchema)vertexSchema.get());
        }

        return Optional.empty();
    }

    protected String getEdgeId(Vertex outVertex, Vertex inVertex) {
        return Integer.toHexString(outVertex.id().hashCode()) + Integer.toHexString(inVertex.id().hashCode());
    }
    //endregion

    //region Fields
    protected UniGraph graph;
    protected Direction direction;
    protected GraphElementSchemaProvider schemaProvider;
    protected TraversalIdProvider<String> traversalIdProvider;
    //endregion
}
