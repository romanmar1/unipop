package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.VertexController;
import org.unipop.elastic.controller.schema.helpers.HasContainersQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseVertexController implements VertexController {
    //region ctor
    public PromiseVertexController(UniGraph graph, VertexController innerVertexController, ElementConverter<Element, Element> elementConverter) {
        this.graph = graph;
        this.innerVertexController = innerVertexController;
        this.elementConverter = elementConverter;
    }
    //endregion


    //region VertexController Implementation
    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        if (predicates.hasContainers == null || predicates.hasContainers.size() == 0) {
            // promise all vertices
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(innerVertexController.vertices(predicates), 0), false)
                    .flatMap(vertex -> StreamSupport.stream(this.elementConverter.convert(vertex).spliterator(), false))
                    .map(BaseVertex.class::cast)
                    .iterator();
        } else if (predicates.hasContainers.size() > 1){
            throw new UnsupportedOperationException("Exactly one Has expected. Usage: has(\"promise\", ..., ...)");
        } else {
            HasContainer hasContainer = predicates.hasContainers.get(0);
            if (!hasContainer.getKey().toLowerCase().equals("promise")) {
                throw new UnsupportedOperationException("Promise Has expected. Usage: has(\"promise\", ..., ...)");
            }

            //I think Compare.neq also could be used...
            /*if ((hasContainer.getPredicate().getValue() != Compare.eq)
                    && (hasContainer.getPredicate().getValue() != Contains.within)) {
                throw new IllegalArgumentException("Promise should be used only with \"P.eq\" or \"Contains.within\" predicates.");
            }*/

            Stream<Promise> promiseStream = Stream.empty();
            if (hasContainer.getBiPredicate() == Compare.eq) {
                promiseStream = Arrays.stream(new Promise[] {(Promise)hasContainer.getPredicate().getValue()});
            } else { // within
                if (Iterable.class.isAssignableFrom(hasContainer.getPredicate().getValue().getClass())) {
                    promiseStream = StreamSupport.stream(((Iterable<Promise>)hasContainer.getPredicate().getValue()).spliterator(), false);
                } else if (Promise[].class.isAssignableFrom(hasContainer.getPredicate().getValue().getClass())) {
                    promiseStream = Arrays.stream((Promise[])hasContainer.getPredicate().getValue());
                }
            }

            return promiseStream.map(promise -> new PromiseVertex(promise, graph)).map(BaseVertex.class::cast).iterator();
        }
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
        return this.innerVertexController.addVertex(id, label, properties);
    }
    //endregion

    //region Private Methods
    protected void translateHasContainers(SearchBuilder searchBuilder, ArrayList<HasContainer> hasContainers) {
        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();
        for (HasContainer hasContainer : hasContainers) {
            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool();
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, searchBuilder.getQueryBuilder(), hasContainer);
        }
    }
    //endregion

    //region fields
    private UniGraph graph;
    private VertexController innerVertexController;
    private ElementConverter<Element, Element> elementConverter;
    //endregion
}
