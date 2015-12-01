package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.dual.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity.PromiseFilterSimilarityQueryAppender;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity.PromiseTypesSimilarityQueryAppender;
import org.unipop.elastic.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchHitScrollIterable;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.CompositeQueryAppender;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 12/1/2015.
 */
public class PromiseSimilarityEdgeController implements EdgeController {
    //region Constructor
    public PromiseSimilarityEdgeController(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Client client,
            ElasticMutations elasticMutations,
            ElasticGraphConfiguration elasticGraphConfiguration,
            ElementConverter<Element, Element> elementConverter) {
        this.graph = graph;
        this.elementConverter = elementConverter;
        this.schemaProvider = schemaProvider;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.configuration = elasticGraphConfiguration;
    }
    //endregion

    //region EdgeController Implementation
    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        throw new UnsupportedOperationException("Promise similarity does not support edge browsing");
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        this.elasticMutations.refreshIfDirty();

        Iterable<IdPromise> bulkIdPromises = getBulkIdPromises(Seq.of(vertices).cast(PromiseVertex.class).toList());
        Iterable<TraversalPromise> bulkTraversalPromises = getBulkTraversalPromises(Seq.of(vertices).cast(PromiseVertex.class).toList());

        SearchBuilder searchBuilder = buildIdPromiseSimilarityEdgeQuery(bulkIdPromises, direction);

        // search builder will be null if the appender failed to append query on the current bulk
        if (searchBuilder == null) {
            return Collections.emptyIterator();
        }

        getSearchHits(searchBuilder);

        return null;

    }

    @Override
    public long edgeCount(Predicates predicates) {
        throw new UnsupportedOperationException("Promise similarity does not support count");
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        throw new UnsupportedOperationException("Promise similarity does not support count");
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new UnsupportedOperationException("Promise similarity does not support group by");
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new UnsupportedOperationException("Promise similarity does not support group by");
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        throw new UnsupportedOperationException("don't make a promise you can't keep");
    }
    //endregion

    //region Private Methods
    private Iterable<IdPromise> getBulkIdPromises(Iterable<PromiseVertex> outPromises) {
        return StreamSupport.stream(outPromises.spliterator(), false)
                .map(vertex -> vertex.getPromise())
                .filter(promise -> promise instanceof IdPromise)
                .map(IdPromise.class::cast)
                .collect(Collectors.toList());
    }

    private List<TraversalPromise> getBulkTraversalPromises(Iterable<PromiseVertex> outPromises) {
        return StreamSupport.stream(outPromises.spliterator(), false)
                .map(vertex -> vertex.getPromise())
                .filter(promise -> promise instanceof TraversalPromise)
                .map(TraversalPromise.class::cast)
                .collect(Collectors.toList());
    }

    private SearchBuilder buildIdPromiseSimilarityEdgeQuery(Iterable<IdPromise> idPromises, Direction direction) {
        SearchBuilder searchBuilder = new SearchBuilder();
        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();

        PromiseBulkInput promiseBulkInput = new PromiseBulkInput(
                idPromises,
                Collections.emptyList(),
                Collections.emptyList(),
                Seq.seq(this.schemaProvider.getVertexTypes()).toList(),
                searchBuilder);

        if (!getIdPromiseQueryAppender(direction).append(promiseBulkInput)) {
            return null;
        }

        return searchBuilder;
    }

    private QueryAppender<PromiseBulkInput> getIdPromiseQueryAppender(Direction direction) {
        QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> idPromiseQueryBuilderFactory = new IdPromiseVertexQueryBuilderFactory();
        QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory = new TraversalPromiseVertexQueryBuilderFactory();

        return new CompositeQueryAppender<PromiseBulkInput>(
                CompositeQueryAppender.Mode.All,
                new PromiseTypesSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                new PromiseFilterSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory));

    }

    protected Iterable<SearchHit> getSearchHits(SearchBuilder searchBuilder) {
        SearchRequestBuilder searchRequest = searchBuilder.getSearchRequest(client);
        if (searchRequest == null) {
            return Collections.emptyList();
        }

        Iterable<SearchHit> scrollIterable = new SearchHitScrollIterable(
                configuration,
                searchRequest,
                searchBuilder.getLimit(),
                client);

        return scrollIterable;
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private ElasticGraphConfiguration configuration;
    private ElementConverter<Element, Element> elementConverter;
    private GraphElementSchemaProvider schemaProvider;
    private Client client;
    private ElasticMutations elasticMutations;
    //endregion
}
