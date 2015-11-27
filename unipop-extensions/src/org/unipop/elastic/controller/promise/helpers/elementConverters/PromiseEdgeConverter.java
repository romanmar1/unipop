package org.unipop.elastic.controller.promise.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.Promise;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.UniGraph;

import java.util.Arrays;

/**
 * Created by Roman on 11/17/2015.
 */
public class PromiseEdgeConverter implements ElementConverter<Element, Element> {
    //region Constructor
    public PromiseEdgeConverter(UniGraph graph) {
        this.graph = graph;
    }
    //endregion

    @Override
    public boolean canConvert(Element element) {
        return Edge.class.isAssignableFrom(element.getClass());
    }

    @Override
    public Iterable<Element> convert(Element element) {
        Edge edge = (Edge)element;
        Vertex outVertex = edge.outVertex();
        Vertex inVertex = edge.inVertex();

        return Arrays.asList(
                new PromiseEdge(
                        element.id(),
                        new PromiseVertex(new IdPromise(outVertex.id()), this.graph),
                        new PromiseVertex(new IdPromise(inVertex.id()), this.graph),
                        this.graph));
    }

    //region Fields
    private UniGraph graph;
    //endregion
}
