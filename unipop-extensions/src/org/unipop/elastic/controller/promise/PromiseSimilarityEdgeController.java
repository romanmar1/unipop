package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.kryo.serializers.FieldSerializer;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.promise.helpers.elementConverters.similarity.TraversalToTraversalSimilarityMapEdgeConverter;
import org.unipop.elastic.controller.promise.helpers.elementConverters.similarity.VertexToTraversalSimilarityEdgeConverter;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseSimilarityBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.dual.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalExpressionIdProvider;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalHashIdProvider;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalIdProvider;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity.PromiseFilterSimilarityQueryAppender;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity.PromiseTypesSimilarityQueryAppender;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity.TraversalPromiseSimilarityQueryAppender;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseVertexSchema;
import org.unipop.elastic.controller.schema.helpers.*;
import org.unipop.elastic.controller.schema.helpers.aggregationConverters.CompositeAggregation;
import org.unipop.elastic.controller.schema.helpers.elementConverters.CompositeElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.ElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.RecycledElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.SearchHitElement;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.CompositeQueryAppender;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.elastic.helpers.AggregationHelper;
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
            ElasticGraphConfiguration elasticGraphConfiguration) {
        this.graph = graph;
        this.schemaProvider = schemaProvider;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.configuration = elasticGraphConfiguration;

        this.searchHitElementFactory = new RecycledElementFactory<>(Arrays.asList(
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph)
        ));
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

        Iterable<BaseEdge> idPromiseSimilarityEdges = Collections.emptyList();
        if (Seq.seq(bulkIdPromises).isNotEmpty()) {
            SearchBuilder searchBuilder = buildIdPromiseSimilarityEdgeQuery(bulkIdPromises, direction);

            // search builder will be null if the appender failed to append query on the current bulk
            if (searchBuilder != null) {
                ElementConverter<Element, Element> idPromiseElementConverter = getIdPromiseElementConverter(direction);
                idPromiseSimilarityEdges = Seq.seq(getSearchHits(searchBuilder))
                        .map(searchHit -> this.searchHitElementFactory.getElement(searchHit))
                        .flatMap(element -> Seq.seq(idPromiseElementConverter.convert(element)))
                        .cast(BaseEdge.class);
            }
        }

        Iterable<BaseEdge> traversalPromiseSimilarityEdges = Collections.emptyList();
        if (Seq.seq(bulkTraversalPromises).isNotEmpty()) {
            SearchBuilder searchBuilder = buildTraversalPromiseSimilarityEdgeQuery(bulkTraversalPromises, direction);

            // search builder will be null if the appender failed to append query on the current bulk
            if (searchBuilder != null) {
                ElementConverter<Map<String, Object>, Element> traversalPromiseElementConverter = getTraversalPromiseElementConverter(direction, bulkTraversalPromises);

                SearchAggregationIterable aggregations = new SearchAggregationIterable(
                        this.graph,
                        searchBuilder.getSearchRequest(this.client),
                        this.client);
                CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

                Map<String, Object> result =
                        AggregationHelper.getAggregationConverter(searchBuilder.getAggregationBuilder(), false)
                                .convert(compositeAggregation);

                traversalPromiseSimilarityEdges = Seq.seq(traversalPromiseElementConverter.convert(result)).cast(BaseEdge.class);
            }
        }

        return Seq.seq(idPromiseSimilarityEdges).concat(Seq.seq(traversalPromiseSimilarityEdges)).iterator();
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
        Optional<GraphPromiseVertexSchema> graphPromiseVertexSchema = getGraphPromiseVertexSchema();
        if (!graphPromiseVertexSchema.isPresent()) {
            return null;
        }

        SearchBuilder searchBuilder = new SearchBuilder();
        translateLabelsPredicate(Collections.emptyList(), searchBuilder, Vertex.class);
        searchBuilder.getIncludeSourceFields().addAll(Seq.seq(graphPromiseVertexSchema.get().getSimilarity().getSimilarityProperties()).toList());
        searchBuilder.setLimit(Seq.seq(idPromises).count());

        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();

        PromiseSimilarityBulkInput promiseSimilarityBulkInput = new PromiseSimilarityBulkInput(
                idPromises,
                Collections.emptyList(),
                Seq.seq(this.schemaProvider.getVertexTypes()).toList(),
                searchBuilder);

        if (!getIdPromiseQueryAppender(direction).append(promiseSimilarityBulkInput)) {
            return null;
        }

        return searchBuilder;
    }

    private SearchBuilder buildTraversalPromiseSimilarityEdgeQuery(Iterable<TraversalPromise> traversalPromises, Direction direction) {
        SearchBuilder searchBuilder = new SearchBuilder();
        translateLabelsPredicate(Collections.emptyList(), searchBuilder, Vertex.class);
        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();

        PromiseSimilarityBulkInput promiseSimilarityBulkInput = new PromiseSimilarityBulkInput(
                Collections.emptyList(),
                traversalPromises,
                Seq.seq(this.schemaProvider.getVertexTypes()).toList(),
                searchBuilder);

        if (!getTraversalPromiseQueryAppender(direction).append(promiseSimilarityBulkInput)) {
            return null;
        }

        return searchBuilder;
    }

    private QueryAppender<PromiseSimilarityBulkInput> getIdPromiseQueryAppender(Direction direction) {
        QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> idPromiseQueryBuilderFactory = new IdPromiseVertexQueryBuilderFactory();
        QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory = new TraversalPromiseVertexQueryBuilderFactory();

        return new CompositeQueryAppender<PromiseSimilarityBulkInput>(
                CompositeQueryAppender.Mode.All,
                new PromiseTypesSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                new PromiseFilterSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory));

    }

    private QueryAppender<PromiseSimilarityBulkInput> getTraversalPromiseQueryAppender(Direction direction) {
        QueryBuilderFactory<IdPromiseSchemaInput<GraphVertexSchema>> idPromiseQueryBuilderFactory = new IdPromiseVertexQueryBuilderFactory();
        QueryBuilderFactory<TraversalPromiseVertexInput> traversalPromiseQueryBuilderFactory = new TraversalPromiseVertexQueryBuilderFactory();

        return new CompositeQueryAppender<PromiseSimilarityBulkInput>(
                CompositeQueryAppender.Mode.All,
                new PromiseTypesSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                new PromiseFilterSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory),
                new TraversalPromiseSimilarityQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), traversalPromiseQueryBuilderFactory));
    }

    private ElementConverter<Element, Element> getIdPromiseElementConverter(Direction direction) {
        try {
            TraversalIdProvider<String> traversalIdProvider = new TraversalHashIdProvider(new TraversalExpressionIdProvider(), "MD5");
            return new CompositeElementConverter<Element, Element>(
                    CompositeElementConverter.Mode.All,
                    new VertexToTraversalSimilarityEdgeConverter(this.graph, direction, this.schemaProvider, traversalIdProvider));
        } catch(Exception ex) {
            return null;
        }
    }

    private ElementConverter<Map<String, Object>, Element> getTraversalPromiseElementConverter(Direction direction, Iterable<TraversalPromise> bulkTraversalPromises) {
        try {
            TraversalIdProvider<String> traversalIdProvider = new TraversalHashIdProvider(new TraversalExpressionIdProvider(), "MD5");
            return new CompositeElementConverter<Map<String, Object>, Element>(
                    CompositeElementConverter.Mode.All,
                    new TraversalToTraversalSimilarityMapEdgeConverter(this.graph, direction, bulkTraversalPromises, this.schemaProvider, traversalIdProvider));
        } catch(Exception ex) {
            return null;
        }
    }

    private Optional<GraphPromiseVertexSchema> getGraphPromiseVertexSchema() {
        Optional<GraphVertexSchema> vertexSchema = this.schemaProvider.getVertexSchema("promise");
        if (vertexSchema.isPresent() && GraphPromiseVertexSchema.class.isAssignableFrom(vertexSchema.get().getClass())) {
            return Optional.of((GraphPromiseVertexSchema)vertexSchema.get());
        }

        return Optional.empty();
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

    @SuppressWarnings("Duplicates")
    private void translateLabelsPredicate(Iterable<String> labels, SearchBuilder searchBuilder, Class elementType) {
        if (labels != null && Seq.seq(labels).isNotEmpty()) {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, labels, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, labels, elementType);
        } else {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, elementType);
        }
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private ElasticGraphConfiguration configuration;
    private GraphElementSchemaProvider schemaProvider;
    private Client client;
    private ElasticMutations elasticMutations;
    protected ElementFactory<SearchHit, SearchHitElement> searchHitElementFactory;
    //endregion
}
