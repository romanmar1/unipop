package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Created by Roman on 11/27/2015.
 */
public abstract class GraphPromiseMapEdgeConverterBase implements ElementConverter<Map<String, Object>, Element> {
    //region Constructor
    public GraphPromiseMapEdgeConverterBase(
            UniGraph graph,
            Direction direction,
            Iterable<HasContainer> edgeAggPromiseHasContainers,
            GraphElementSchemaProvider schemaProvider) {
        this.graph = graph;
        this.direction = direction;
        this.edgeAggPromiseHasContainers = edgeAggPromiseHasContainers;
        this.schemaProvider = schemaProvider;
    }
    //endregion

    //region Protected Methods
    protected String getEdgeId(Vertex outVertex, Vertex inVertex) {
        return Integer.toHexString(outVertex.id().hashCode()) + Integer.toHexString(inVertex.id().hashCode());
    }
    //endregion

    //region Protected Methods
    protected Iterable<GraphPromiseEdgeSchema.Property> getPromiseProperties() {
        return getPromiseEdgeSchema().isPresent() ? getPromiseEdgeSchema().get().getProperties() : Collections.emptyList();
    }

    protected Optional<GraphPromiseEdgeSchema> getPromiseEdgeSchema() {
        Optional<GraphEdgeSchema> edgeSchema = this.schemaProvider.getEdgeSchema("promise", Optional.of("promise"), Optional.of("promise"));
        if (edgeSchema.isPresent() && GraphPromiseEdgeSchema.class.isAssignableFrom(edgeSchema.get().getClass())) {
            return Optional.of((GraphPromiseEdgeSchema)edgeSchema.get());
        }

        return Optional.empty();
    }
    //endregion

    //region Fields
    protected UniGraph graph;
    protected Direction direction;
    protected Iterable<HasContainer> edgeAggPromiseHasContainers;
    protected GraphElementSchemaProvider schemaProvider;
    //endregion
}
