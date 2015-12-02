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
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;

/**
 * Created by Roman on 12/1/2015.
 */
public class VertexToTraversalSimilarityEdgeConverter extends GraphPromiseSimilarityEdgeConverterBase<Element> {
    //region Constructor
    public VertexToTraversalSimilarityEdgeConverter(
            UniGraph graph,
            Direction direction,
            GraphElementSchemaProvider schemaProvider,
            TraversalIdProvider<String> traversalIdProvider) {
        super(graph, direction, schemaProvider, traversalIdProvider);
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

        TraversalPromise traversalPromise = new TraversalPromise(traversalIdProvider.getId(traversal), traversal);
        traversalPromise.setIsStrongId(false);

        PromiseVertex promiseVertex = new PromiseVertex(traversalPromise, this.graph);
        return promiseVertex;
    }
    //endregion
}
