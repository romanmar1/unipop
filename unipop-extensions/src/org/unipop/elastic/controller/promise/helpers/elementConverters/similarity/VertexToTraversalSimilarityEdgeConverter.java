package org.unipop.elastic.controller.promise.helpers.elementConverters.similarity;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalIdProvider;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;

/**
 * Created by Roman on 12/1/2015.
 */
public class VertexToTraversalSimilarityEdgeConverter implements ElementConverter<Element, Element> {
    //region Constructor
    public VertexToTraversalSimilarityEdgeConverter(UniGraph graph, Direction direction, TraversalIdProvider<String> traversalIdProvider) {
        this.graph = graph;
        this.direction = direction;
        this.traversalIdProvider = traversalIdProvider;
    }
    //endregion

    //region ElementConverter Implementation
    @Override
    public boolean canConvert(Element element) {
        return true;
    }

    @Override
    public Iterable<Element> convert(Element element) {
        PromiseVertex idPromiseVertex = new PromiseVertex(new IdPromise(element.id()), this.graph);
        PromiseVertex traversalPromiseVertex = buildTraversalPromiseVertex(element);

        ArrayList<Element> edges = new ArrayList<>();
        switch (this.direction) {
            case OUT:
                edges.add(new PromiseEdge(getEdgeId(idPromiseVertex, traversalPromiseVertex), idPromiseVertex, traversalPromiseVertex, null, this.graph));
                break;

            case IN:
                edges.add(new PromiseEdge(getEdgeId(traversalPromiseVertex, idPromiseVertex), traversalPromiseVertex, idPromiseVertex, null, this.graph));
                break;

            case BOTH:
                edges.add(new PromiseEdge(getEdgeId(idPromiseVertex, traversalPromiseVertex), idPromiseVertex, traversalPromiseVertex, null, this.graph));
                edges.add(new PromiseEdge(getEdgeId(traversalPromiseVertex, idPromiseVertex), traversalPromiseVertex, idPromiseVertex, null, this.graph));
                break;
        }
        return edges;
    }
    //endregion

    //region Protected Methods
    protected PromiseVertex buildTraversalPromiseVertex(Element element) {
        Traversal[] propertyTraversals = Seq.seq(element.properties()).map(property -> __.has(property.key(), P.eq(property.value()))).toArray(Traversal<?, ?>[]::new);
        Traversal traversal = __.or(propertyTraversals);
        PromiseVertex promiseVertex = new PromiseVertex(new TraversalPromise(traversalIdProvider.getId(traversal), traversal), this.graph);
        return promiseVertex;
    }

    protected String getEdgeId(Vertex outVertex, Vertex inVertex) {
        return Integer.toHexString(outVertex.id().hashCode()) + Integer.toHexString(inVertex.id().hashCode());
    }
    //endregion

    //region Fields
    protected UniGraph graph;
    protected Direction direction;
    protected TraversalIdProvider<String> traversalIdProvider;
    //endregion
}
