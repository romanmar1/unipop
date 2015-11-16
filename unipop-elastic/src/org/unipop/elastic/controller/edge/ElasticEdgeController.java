package org.unipop.elastic.controller.edge;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.javatuples.Pair;
import org.unipop.controller.Predicates;
import org.unipop.controller.aggregation.SemanticKeyTraversal;
import org.unipop.controller.aggregation.SemanticReducerTraversal;
import org.unipop.elastic.controller.schema.helpers.AggregationBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchAggregationIterable;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.aggregationConverters.*;
import org.unipop.elastic.helpers.*;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ElasticEdgeController implements org.unipop.controller.EdgeController {
    private UniGraph graph;
    private final Client client;
    private final ElasticMutations elasticMutations;
    private final String indexName;
    private final int scrollSize;
    private TimingAccessor timing;

    public ElasticEdgeController(UniGraph graph, Client client, ElasticMutations elasticMutations, String indexName,
                                 int scrollSize, TimingAccessor timing) {
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.indexName = indexName;
        this.scrollSize = scrollSize;
        this.timing = timing;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        elasticMutations.refresh(indexName);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));

        return new QueryIterator<>(boolFilter, scrollSize, predicates.limitHigh,
                client, this::createEdge, timing, indexName);
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        elasticMutations.refresh(indexName);

        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id();

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        addFiltersByDirection(direction, vertexIds, boolFilter);

        return new QueryIterator<>(boolFilter, scrollSize, predicates.limitHigh, client, this::createEdge, timing, indexName);
    }

    @Override
    public long edgeCount(Predicates predicates) {
        elasticMutations.refresh(indexName);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));

        long count = 0;
        try {
            SearchResponse response = client.prepareSearch().setIndices(indexName)
                    .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                    .setSearchType(SearchType.COUNT).execute().get();
            count += response.getHits().getTotalHits();
        } catch(Exception ex) {
            //TODO: decide what to do here
            return 0L;
        }

        return count;
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        elasticMutations.refresh(indexName);

        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id();

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));


        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        AggregationBuilder aggregationBuilder = new AggregationBuilder();

        if (direction == Direction.IN) {
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
            aggregationBuilder.seekRoot().terms(ElasticEdge.InId).field(ElasticEdge.InId).size(0);
        } else if (direction == Direction.OUT) {
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
            aggregationBuilder.seekRoot().terms(ElasticEdge.OutId).field(ElasticEdge.OutId).size(0);
        }
        else if (direction == Direction.BOTH) {
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds),
                    FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds)));

            aggregationBuilder.seekRoot().terms(ElasticEdge.InId).field(ElasticEdge.InId).size(0);
            aggregationBuilder.seekRoot().terms(ElasticEdge.OutId).field(ElasticEdge.OutId).size(0);
        }

        HashMap<String, Pair<Long, Vertex>> idsCount = AggregationHelper.getIdsCounts(Arrays.asList(vertices));

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(indexName)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setSearchType(SearchType.COUNT);
        for(org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(aggregationBuilder, false).convert(compositeAggregation);

        return AggregationHelper.countResultsWithRespectToOriginalOccurrences(idsCount, result);
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        elasticMutations.refresh(indexName);
        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        boolFilter.must(FilterBuilders.existsFilter(ElasticEdge.InId));

        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        this.applyAggregationBuilder(aggregationBuilder, keyTraversal, reducerTraversal);

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(indexName)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setSearchType(SearchType.COUNT);
        for(org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(aggregationBuilder, true).convert(compositeAggregation);
        return result;
    }

    private void applyAggregationBuilder(AggregationBuilder aggregationBuilder, Traversal keyTraversal, Traversal reducerTraversal) {
            AggregationHelper.applyAggregationBuilder(
                    aggregationBuilder,
                    keyTraversal,
                    reducerTraversal,
                    0,
                    0,
                    "global_ordinal_hash"
            );
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        elasticMutations.refresh(indexName);

        Object[] vertexIds = new Object[vertices.length];
        for(int i = 0; i < vertices.length; i++) vertexIds[i] = vertices[i].id();

        if (edgeLabels != null && edgeLabels.length > 0)
            predicates.hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(edgeLabels)));

        BoolFilterBuilder boolFilter = ElasticHelper.createFilterBuilder(predicates.hasContainers);
        addFiltersByDirection(direction, vertexIds, boolFilter);

        AggregationBuilder aggregationBuilder = new AggregationBuilder();
        this.applyAggregationBuilder(aggregationBuilder, keyTraversal, reducerTraversal);

        SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(indexName)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter))
                .setSearchType(SearchType.COUNT);
        for(org.elasticsearch.search.aggregations.AggregationBuilder innerAggregation : aggregationBuilder.getAggregations()) {
            searchRequest.addAggregation(innerAggregation);
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(this.graph, searchRequest, this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result = AggregationHelper.getAggregationConverter(aggregationBuilder, true).convert(compositeAggregation);
        return result;
    }

    private void addFiltersByDirection(Direction direction, Object[] vertexIds, BoolFilterBuilder boolFilter) {
        if (direction == Direction.IN)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds));
        else if (direction == Direction.OUT)
            boolFilter.must(FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds));
        else if (direction == Direction.BOTH)
            boolFilter.must(FilterBuilders.orFilter(
                    FilterBuilders.termsFilter(ElasticEdge.InId, vertexIds),
                    FilterBuilders.termsFilter(ElasticEdge.OutId, vertexIds)));
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        ElasticEdge elasticEdge = new ElasticEdge(edgeId, label, properties, outV, inV,this, graph, elasticMutations, indexName);
        try {
            elasticMutations.addElement(elasticEdge, indexName, null, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(elasticEdge.id());
        }
        return elasticEdge;
    }

    private BaseEdge createEdge(Object id, String label, Map<String, Object> fields) {
        BaseVertex outV = this.graph.getControllerManager().vertex(Direction.OUT, fields.get(ElasticEdge.OutId), fields.get(ElasticEdge.OutLabel).toString());
        BaseVertex inV = this.graph.getControllerManager().vertex(Direction.IN, fields.get(ElasticEdge.InId), fields.get(ElasticEdge.InLabel).toString());
        BaseEdge edge = new ElasticEdge(id, label, fields, outV, inV, this,  graph, elasticMutations, indexName);
        return edge;
    }
}
