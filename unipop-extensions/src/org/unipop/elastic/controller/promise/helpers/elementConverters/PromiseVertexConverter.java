package org.unipop.elastic.controller.promise.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.promise.Promise;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.structure.UniGraph;

import java.util.Arrays;

/**
 * Created by Roman on 11/17/2015.
 */
public class PromiseVertexConverter implements ElementConverter<Element, Element> {
    //region Constructor
    public PromiseVertexConverter(UniGraph graph) {
        this.graph = graph;
    }
    //endregion

    //region ElementConverter Implementation
    @Override
    public boolean canConvert(Element element) {
        return Vertex.class.isAssignableFrom(element.getClass());
    }

    @Override
    public Iterable<Element> convert(Element element) {
        return Arrays.asList(new PromiseVertex(new IdPromise(element.id(), element.label()), graph));
    }
    //endregion

    //region Fields
    private UniGraph graph;
    //endregion
}
