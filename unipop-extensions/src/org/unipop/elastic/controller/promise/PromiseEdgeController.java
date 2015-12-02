package org.unipop.elastic.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.client.Client;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.elementConverters.map.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.dual.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalConcatKeyIdProvider;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.*;
import org.unipop.elastic.controller.schema.helpers.aggregationConverters.CompositeAggregation;
import org.unipop.elastic.controller.schema.helpers.elementConverters.CompositeElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.CompositeQueryAppender;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.helpers.AggregationHelper;
import org.unipop.elastic.helpers.ElasticMutations;
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
    public PromiseEdgeController(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Client client,
            EdgeController innerEdgeController,
            ElasticMutations elasticMutations,
            ElementConverter<Element, Element> elementConverter) {
        this.graph = graph;
        this.innerEdgeController = innerEdgeController;
        this.elementConverter = elementConverter;
        this.schemaProvider = schemaProvider;
        this.client = client;
        this.elasticMutations = elasticMutations;
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
                .filter(hasContainer -> hasContainer.getKey().toLowerCase().equals(PromiseStrings.HasKeys.IN_PROMISE)).collect(Collectors.toList());
        List<HasContainer> outHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().toLowerCase().equals(PromiseStrings.HasKeys.OUT_PROMISE)).collect(Collectors.toList());
        List<HasContainer> edgeHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> !inHasContainers.contains(hasContainer) && !outHasContainers.contains(hasContainer)).collect(Collectors.toList());

        if (inHasContainers.size() > 1) {
            throw new UnsupportedOperationException("Single \"" + PromiseStrings.HasKeys.IN_PROMISE + "\" allowed");
        }

        if (outHasContainers.size() > 1) {
            throw new UnsupportedOperationException("Single \"" + PromiseStrings.HasKeys.OUT_PROMISE + "\" allowed");
        }

        SearchBuilder searchBuilder = buildEdgePromiseQuery(inHasContainers, outHasContainers, edgeHasContainers);

        return Collections.emptyIterator();
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        this.elasticMutations.refreshIfDirty();

        List<HasContainer> predicatesPromiseHasContainers = predicates.hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().toLowerCase().equals(PromiseStrings.HasKeys.PREDICATES_PROMISE)).collect(Collectors.toList());
        if (predicatesPromiseHasContainers.size() > 1) {
            throw new UnsupportedOperationException("Single \"" + PromiseStrings.HasKeys.PREDICATES_PROMISE + "\" allowed");
        }

        Optional<GraphEdgeSchema> promiseEdgeSchema = this.schemaProvider.getEdgeSchema("promise", Optional.of("promise"), Optional.of("promise"));
        List<GraphPromiseEdgeSchema.Property> promiseProperties =
                !promiseEdgeSchema.isPresent() || GraphPromiseEdgeSchema.class.isAssignableFrom(promiseEdgeSchema.get().getClass()) ?
                    Collections.emptyList() : Seq.seq(((GraphPromiseEdgeSchema)promiseEdgeSchema.get()).getProperties()).toList();

        List<HasContainer> edgeAggPromiseHasContainers = Seq.seq(predicates.hasContainers)
                .filter(hasContainer ->  promiseProperties.contains(hasContainer.getKey())).toList();

        List<HasContainer> edgeHasContainers = Seq.seq(predicates.hasContainers)
                .filter(hasContainer -> !predicatesPromiseHasContainers.contains(hasContainer) && !edgeAggPromiseHasContainers.contains(hasContainer))
                .toList();

        Iterable<IdPromise> bulkIdPromises = getBulkIdPromises(Seq.of(vertices).cast(PromiseVertex.class).toList());
        Iterable<TraversalPromise> bulkTraversalPromises = getBulkTraversalPromises(Seq.of(vertices).cast(PromiseVertex.class).toList());
        Iterable<TraversalPromise> predicatesTraversalPromises = Seq.seq(extractPromises(predicatesPromiseHasContainers)).cast(TraversalPromise.class).toList();

        SearchBuilder searchBuilder = buildBulkEdgePromiseQuery(
                bulkIdPromises,
                bulkTraversalPromises,
                predicatesTraversalPromises,
                edgeLabels,
                edgeAggPromiseHasContainers,
                edgeHasContainers,
                direction);

        // search builder will be null if the appender failed to append query on the current bulk
        if (searchBuilder == null) {
            return Collections.emptyIterator();
        }

        SearchAggregationIterable aggregations = new SearchAggregationIterable(
                this.graph,
                searchBuilder.getSearchRequest(this.client),
                this.client);
        CompositeAggregation compositeAggregation = new CompositeAggregation(null, aggregations);

        Map<String, Object> result =
                AggregationHelper.getAggregationConverter(searchBuilder.getAggregationBuilder(), false)
                .convert(compositeAggregation);

        ElementConverter<Map<String, Object>, Element> elementConverter = this.getElementConverter(
                bulkTraversalPromises,
                predicatesTraversalPromises,
                edgeAggPromiseHasContainers,
                direction);

        return Seq.seq(elementConverter.convert(result)).cast(BaseEdge.class).iterator();
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
    private SearchBuilder buildBulkEdgePromiseQuery(
            Iterable<IdPromise> bulkIdPromises,
            Iterable<TraversalPromise> bulkTraversalPromises,
            Iterable<TraversalPromise> predicatesTraversalPromises,
            String[] edgeLabels,
            Iterable<HasContainer> edgeAggPromiseHasContainers,
            Iterable<HasContainer> edgeHasContainers,
            Direction direction) {
        SearchBuilder searchBuilder = buildPromiseEdgePredicatesQuery(edgeHasContainers, edgeLabels);

        PromiseBulkInput promiseBulkInput = new PromiseBulkInput(
                bulkIdPromises,
                bulkTraversalPromises,
                predicatesTraversalPromises,
                edgeAggPromiseHasContainers,
                searchBuilder.getTypes(),
                searchBuilder);

        QueryAppender<PromiseBulkInput> queryAppender = getQueryAppender(direction);
        if (!queryAppender.append(promiseBulkInput)) {
            return null;
        }

        return searchBuilder;
    }

    private SearchBuilder buildEdgePromiseQuery(
            Iterable<HasContainer> outPromiseHasContainers,
            Iterable<HasContainer> inPromiseHasContainers,
            Iterable<HasContainer> edgeHasContainers) {

        /*SearchBuilder searchBuilder = new SearchBuilder();
        buildPromiseEdgePredicatesQuery(searchBuilder, edgeHasContainers);

        // build first level of aggregation
        Iterable<Promise> outPromises = this.extractPromises(outPromiseHasContainers);
        Iterable<IdPromise> outIdPromises = getIdPromises(outPromises);
        Iterable<TraversalPromise> outTraversalPromises = getTraversalPromises(outPromises);
        buildPromiseEdgeAggregationOutQuery(searchBuilder, outIdPromises, outTraversalPromises);

        // build second level of aggregation
        Iterable<Promise> inPromises = this.extractPromises(inPromiseHasContainers);
        Iterable<IdPromise> inIdPromises = getIdPromises(inPromises);
        Iterable<TraversalPromise> inTraversalPromises = getTraversalPromises(outPromises);
        buildPromiseEdgeAggregationInQuery(searchBuilder, inIdPromises, inTraversalPromises);

        return searchBuilder;*/
        return null;
    }

    private List<TraversalPromise> getBulkTraversalPromises(Iterable<PromiseVertex> outPromises) {
        return StreamSupport.stream(outPromises.spliterator(), false)
                .map(vertex -> vertex.getPromise())
                .filter(promise -> promise instanceof TraversalPromise)
                .map(TraversalPromise.class::cast)
                .collect(Collectors.toList());
    }

    private Iterable<IdPromise> getBulkIdPromises(Iterable<PromiseVertex> outPromises) {
        return StreamSupport.stream(outPromises.spliterator(), false)
                .map(vertex -> vertex.getPromise())
                .filter(promise -> promise instanceof IdPromise)
                .map(IdPromise.class::cast)
                .collect(Collectors.toList());
    }

    private Iterable<Promise> extractPromises(Iterable<HasContainer> hasContainers) {
        List<String> relevantPromiseKeys = Arrays.asList(
                PromiseStrings.HasKeys.IN_PROMISE,
                PromiseStrings.HasKeys.OUT_PROMISE,
                PromiseStrings.HasKeys.PREDICATES_PROMISE);
        return StreamSupport.stream(hasContainers.spliterator(), false)
                .filter(hasContainer -> relevantPromiseKeys.contains(hasContainer.getKey().toLowerCase()))
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

    private SearchBuilder buildPromiseEdgePredicatesQuery(Iterable<HasContainer> edgeHasContainers, String[] edgeLabels) {
        SearchBuilder searchBuilder = new SearchBuilder();
        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();
        translateHasContainers(searchBuilder, edgeHasContainers);
        translateLabelsPredicate(Arrays.stream(edgeLabels).collect(Collectors.toList()), searchBuilder, Edge.class);
        return searchBuilder;
    }

    protected void translateHasContainers(SearchBuilder searchBuilder, Iterable<HasContainer> hasContainers) {
        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();
        for (HasContainer hasContainer : hasContainers) {
            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool().must();
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, searchBuilder.getQueryBuilder(), hasContainer);
        }
    }

    private void buildPromiseEdgeAggregationOutQuery (
            SearchBuilder searchBuilder,
            Iterable<IdPromise> outIdPromises,
            Iterable<TraversalPromise> outTraversalPromises) {

        /*TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator();
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
                .terms("outIdsTerms").field("entityOutId").size(0).shardSize(0).executionHint("global_ordinals_hash");*/
    }

    private void buildPromiseEdgeAggregationInQuery (
            SearchBuilder searchBuilder,
            Iterable<IdPromise> inIdPromises,
            Iterable<TraversalPromise> inTraversalPromises) {

    }

    @SuppressWarnings("Duplicates")
    private Map<Class, List<Promise>> getPartionedPromiseMapByClass(Iterable<PromiseVertex> vertices) {
        return StreamSupport.stream(vertices.spliterator(), false)
                .collect(Collectors.toMap(
                        vertex -> vertex.getPromise().getClass(),
                        vertex -> new ArrayList<>(Arrays.asList(vertex.getPromise())),
                        (list1, list2) -> {
                            if (list1.size() > list2.size()) {
                                list1.addAll(list2);
                                return list1;
                            } else {
                                list2.addAll(list1);
                                return list2;
                            }
                        }));
    }

    private QueryAppender<PromiseBulkInput> getQueryAppender(Direction direction) {
        QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory = new CachedTraversalPromiseEdgeQueryBuilderFactory<>(
                new TraversalPromiseEdgeQueryBuilderFactory(new TraversalConcatKeyIdProvider())
        );

        QueryBuilderFactory<IdPromiseSchemaInput<GraphEdgeSchema>> idPromiseQueryBuilderFactory = new IdPromiseEdgeQueryBuilderFactory();

        return new CompositeQueryAppender<PromiseBulkInput>(
                    CompositeQueryAppender.Mode.All,
                    new DualPromiseTypesQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                    new DualPromiseDirectionQueryAppender(this.graph, this.schemaProvider, Optional.of(direction)),
                    new DualPromiseFilterQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory),
                    new DualIdPromiseAggregationQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory),
                    new DualTraversalPromiseAggregationQueryAppender(this.graph, this.schemaProvider, Optional.of(direction), traversalPromiseQueryBuilderFactory));

    }

    @SuppressWarnings("Duplicates")
    private void translateLabelsPredicate(Iterable<String> labels, SearchBuilder searchBuilder, Class elementType) {
        if (labels != null && StreamSupport.stream(labels.spliterator(), false).count() > 0) {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, labels, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, labels, elementType);
        } else {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, elementType);
        }
    }

    private ElementConverter<Map<String, Object>, Element> getElementConverter(
            Iterable<TraversalPromise> bulkTraversalPromises,
            Iterable<TraversalPromise> predicatesTraversalPromises,
            Iterable<HasContainer> edgeAggPromiseHasContainers,
            Direction direction) {
        return new MapDistinctEdgeConverter(this.graph, direction, edgeAggPromiseHasContainers, this.schemaProvider,
            new CompositeElementConverter<Map<String, Object>, Element>(
                    CompositeElementConverter.Mode.All,
                    new IdToIdMapEdgeConverter(this.graph, direction, edgeAggPromiseHasContainers, this.schemaProvider),
                    new IdToTraversalMapEdgeConverter(this.graph, direction, edgeAggPromiseHasContainers, this.schemaProvider, predicatesTraversalPromises),
                    new TraversalToIdMapEdgeConverter(this.graph, direction, edgeAggPromiseHasContainers, this.schemaProvider, bulkTraversalPromises),
                    new TraversalToTraversalMapEdgeConverter(this.graph, direction, edgeAggPromiseHasContainers, this.schemaProvider, bulkTraversalPromises, predicatesTraversalPromises)));
    }
    //endregion

    //region Fields
    private UniGraph graph;
    private EdgeController innerEdgeController;
    private ElementConverter<Element, Element> elementConverter;
    private GraphElementSchemaProvider schemaProvider;
    private Client client;
    private ElasticMutations elasticMutations;
    //endregion
}
