package org.unipop.extensions.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseEdgeController implements EdgeController {
    //region EdgeController Implementation
    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        return null;
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
        throw new UnsupportedOperationException("don't make a promise you can't keep");
    }
    //endregion
}
