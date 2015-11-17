package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseEdgeController implements EdgeController {
    //region Constructor
    public PromiseEdgeController(UniGraph graph, EdgeController innerEdgeController, ElementConverter<Element, Element> elementConverter) {
        this.graph = graph;
        this.innerEdgeController = innerEdgeController;
        this.elementConverter = elementConverter;
    }
    //endregion

    //region EdgeController Implementation
    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        if (predicates.hasContainers == null || predicates.hasContainers.size() == 0) {
            // promise all edges
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this.innerEdgeController.edges(predicates), 0), false)
                    .flatMap(edge -> StreamSupport.stream(this.elementConverter.convert(edge).spliterator(), false).map(BaseEdge.class::cast))
                    .iterator();
        } else {
            //TODO: implement actual promise edges
            return null;
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        return null;
    }

    @Override
    public long edgeCount(Predicates predicates) {
        throw new UnsupportedOperationException("edge count not supported in promise land");
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        throw new UnsupportedOperationException("edge count not supported in promise land");
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new UnsupportedOperationException("edge group by not supported in promise land");
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new UnsupportedOperationException("edge group by not supported in promise land");
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return this.innerEdgeController.addEdge(edgeId, label, outV, inV, properties);
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private EdgeController innerEdgeController;
    private ElementConverter<Element, Element> elementConverter;
    //endregion
}
