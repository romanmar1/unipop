package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.UniGraph;

import java.util.Map;

/**
 * Created by Roman on 11/27/2015.
 */
public class MapDistinctEdgeConverter extends GraphPromiseMapEdgeConverterBase {
    //region Constructor
    public MapDistinctEdgeConverter(UniGraph graph, Direction direction, ElementConverter<Map<String, Object>, Element> innerConverter) {
        super(graph, direction);
        this.innerConverter = innerConverter;
    }
    //endregion

    //region GraphPromiseMapEdgeConverterBase Implementation
    @Override
    public boolean canConvert(Map<String, Object> map) {
        return innerConverter.canConvert(map);
    }

    @Override
    public Iterable<Element> convert(Map<String, Object> map) {
        Iterable<Element> elements = innerConverter.convert(map);

        if (direction == Direction.BOTH) {
            return Seq.seq(elements).distinct(element -> {
                Vertex outVertex = ((Edge) element).outVertex();
                Vertex inVertex = ((Edge) element).inVertex();

                return outVertex.id().hashCode() > inVertex.id().hashCode() ?
                        outVertex.id().toString() + inVertex.id().toString() :
                        inVertex.id().toString() + outVertex.id().toString();
            });
        }

        return elements;
    }
    //endregion

    //region Fields
    private ElementConverter<Map<String, Object>, Element> innerConverter;
    //endregion
}
