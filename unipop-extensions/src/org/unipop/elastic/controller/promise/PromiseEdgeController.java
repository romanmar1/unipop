package org.unipop.elastic.controller.promise;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.schema.helpers.HasContainersQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
                    .flatMap(edge -> StreamSupport.stream(this.elementConverter.convert(edge).spliterator(), false))
                    .map(BaseEdge.class::cast)
                    .iterator();
        }

        List<HasContainer> inHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().toLowerCase().equals(IN_PROMISE)).collect(Collectors.toList());
        List<HasContainer> outHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().toLowerCase().equals(OUT_PROMISE)).collect(Collectors.toList());
        List<HasContainer> edgeHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> !inHasContainers.contains(hasContainer) && !outHasContainers.contains(hasContainer)).collect(Collectors.toList());

        if (inHasContainers.size() > 1) {
            throw new UnsupportedOperationException("Single \"" + IN_PROMISE + "\" allowed");
        }

        if (outHasContainers.size() > 1) {
            throw new UnsupportedOperationException("Single \"" + OUT_PROMISE + "\" allowed");
        }

        SearchBuilder searchBuilder = buildEdgePromiseQuery(inHasContainers, outHasContainers, edgeHasContainers);

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
        return this.innerEdgeController.addEdge(edgeId, label, outV, inV, properties);
    }
    //endregion

    //region Private Methods
    private SearchBuilder buildEdgePromiseQuery(
            Iterable<HasContainer> outPromiseHasContainers,
            Iterable<HasContainer> inPromiseHasContainers,
            Iterable<HasContainer> edgeHasContainers) {

        SearchBuilder searchBuilder = new SearchBuilder();
        buildPromiseEdgePredicatesQuery(searchBuilder, edgeHasContainers);

        Iterable<Promise> outPromises = this.extractPromises(outPromiseHasContainers);
        Iterable<IdPromise> outIdPromises = StreamSupport.stream(outPromises.spliterator(), false)
                .filter(promise -> promise instanceof IdPromise)
                .map(IdPromise.class::cast)
                .collect(Collectors.toList());
        Iterable<TraversalPromise> outTraversalPromises = StreamSupport.stream(outPromises.spliterator(), false)
                .filter(promise -> promise instanceof TraversalPromise)
                .map(TraversalPromise.class::cast)
                .collect(Collectors.toList());

        buildPromiseEdgeAggregationOutQuery(searchBuilder, outIdPromises, outTraversalPromises);

        Iterable<Promise> inPromises = this.extractPromises(inPromiseHasContainers);
        Iterable<IdPromise> inIdPromises = StreamSupport.stream(inPromises.spliterator(), false)
                .filter(promise -> promise instanceof IdPromise)
                .map(IdPromise.class::cast)
                .collect(Collectors.toList());
        Iterable<TraversalPromise> inTraversalPromises = StreamSupport.stream(outPromises.spliterator(), false)
                .filter(promise -> promise instanceof TraversalPromise)
                .map(TraversalPromise.class::cast)
                .collect(Collectors.toList());

        buildPromiseEdgeAggregationInQuery(searchBuilder, inIdPromises, inTraversalPromises);

        return searchBuilder;
    }

    private Iterable<Promise> extractPromises(Iterable<HasContainer> hasContainers) {
        return StreamSupport.stream(hasContainers.spliterator(), false)
                .filter(hasContainer -> Arrays.asList(IN_PROMISE, OUT_PROMISE).contains(hasContainer.getKey().toLowerCase()))
                .flatMap(hasContainer -> {
                    if (Promise.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                        return Arrays.asList((Promise)hasContainer.getValue()).stream();
                    } else if (Promise[].class.isAssignableFrom(hasContainer.getValue().getClass())) {
                        return Arrays.<Promise>stream((Promise[])hasContainer.getValue());
                    } else if (Iterable.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                        return StreamSupport.<Promise>stream(((Iterable<Promise>)hasContainer.getValue()).spliterator(), false);
                    } else {
                        return Stream.empty();
                    }
                }).collect(Collectors.toList());
    }

    private void buildPromiseEdgePredicatesQuery(SearchBuilder searchBuilder, Iterable<HasContainer> edgeHasContainers) {
        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();
        translateHasContainers(searchBuilder, edgeHasContainers);
    }

    protected void translateHasContainers(SearchBuilder searchBuilder, Iterable<HasContainer> hasContainers) {
        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();
        for (HasContainer hasContainer : hasContainers) {
            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool();
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, searchBuilder.getQueryBuilder(), hasContainer);
        }
    }

    private void buildPromiseEdgeAggregationOutQuery (
            SearchBuilder searchBuilder,
            Iterable<IdPromise> outIdPromises,
            Iterable<TraversalPromise> outTraversalPromises) {

        TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator();
        for (TraversalPromise traversalPromise : outTraversalPromises) {
            QueryBuilder queryBuilder = new QueryBuilder();
            traversalQueryTranslator.applyTraversal(searchBuilder, queryBuilder, traversalPromise.getTraversal());
            searchBuilder.getAggregationBuilder().filters("outTraversalPromisesFilters").filter(traversalPromise.getId().toString(), queryBuilder);
        }

        Iterable<String> outIds = StreamSupport.stream(outIdPromises.spliterator(), false)
                .map(IdPromise::getId)
                .map(Object::toString)
                .collect(Collectors.toList());

        //todo : add appenderes for ids
        QueryBuilder outIdsPromisesQueryBuilder = new QueryBuilder().seekRoot().query().filtered().filter().bool().should();
        searchBuilder.getAggregationBuilder().seekRoot().filters("outIdsPromisesFilters")
                .filter("outIdsPromisesFilter", outIdsPromisesQueryBuilder)
                .seek("outIdsPromisesFilters")
                .terms("outIdsTerms").field("entityOutId").size(0).shardSize(0).executionHint("global_ordinals_hash");
    }

    private void buildPromiseEdgeAggregationInQuery (
            SearchBuilder searchBuilder,
            Iterable<IdPromise> inIdPromises,
            Iterable<TraversalPromise> inTraversalPromises) {

    }
    //endregion

    //region Fields
    private UniGraph graph;
    private EdgeController innerEdgeController;
    private ElementConverter<Element, Element> elementConverter;
    private final String IN_PROMISE = "inpromise";
    private final String OUT_PROMISE = "outpromise";
    //endregion
}
