package org.unipop.extensions.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseVertexController implements VertexController {
    //region VertexController Implementation
    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        return null;
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        throw new UnsupportedOperationException("Not needed in interface");
    }

    @Override
    public long vertexCount(Predicates predicates) {
        throw new UnsupportedOperationException("vertex count not supported in promise land");
    }

    @Override
    public Map<String, Object> vertexGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new UnsupportedOperationException("vertex group by not supported in promise land");
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        throw new UnsupportedOperationException("don't make a promise you can't keep");
    }
    //endregion
}
