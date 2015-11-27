package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by Roman on 11/27/2015.
 */
public abstract class GraphPromiseMapEdgeConverterBase implements ElementConverter<Map<String, Object>, Element> {
    //region Constructor
    public GraphPromiseMapEdgeConverterBase(UniGraph graph, Direction direction) {
        this.graph = graph;
        this.direction = direction;
    }
    //endregion

    //region Protected Methods
    protected String getEdgeId(Vertex outVertex, Vertex inVertex) {
        return Integer.toHexString(outVertex.id().hashCode()) + Integer.toHexString(inVertex.id().hashCode());
    }
    //endregion

    //region Fields
    protected UniGraph graph;
    protected Direction direction;
    //endregion
}
