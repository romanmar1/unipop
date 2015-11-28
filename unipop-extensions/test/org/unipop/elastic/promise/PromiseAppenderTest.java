package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.junit.Before;
import org.junit.Test;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.dual.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.*;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalConcatIdProvider;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.CompositeQueryAppender;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */
public class PromiseAppenderTest {
    @Before
    public void setup() {
        QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory = new CachedTraversalPromiseEdgeQueryBuilderFactory<>(
                new TraversalPromiseEdgeQueryBuilderFactory(new TraversalConcatIdProvider())
        );

        QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory = new IdPromiseEdgeQueryBuilderFactory();

        this.queryAppender =
                new CompositeQueryAppender<PromiseBulkInput>(
                    CompositeQueryAppender.Mode.All,
                    new DualPromiseTypesQueryAppender(null, new SimpleSchemaProvider(), Optional.of(Direction.OUT)),
                    new DualPromiseDirectionQueryAppender(null, new SimpleSchemaProvider(), Optional.of(Direction.OUT)),
                    new DualPromiseFilterQueryAppender(null, new SimpleSchemaProvider(), Optional.of(Direction.OUT), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory),
                    new DualIdPromiseAggregationQueryAppender(null, new SimpleSchemaProvider(), Optional.of(Direction.OUT), idPromiseQueryBuilderFactory, traversalPromiseQueryBuilderFactory),
                    new DualTraversalPromiseAggregationQueryAppender(null, new SimpleSchemaProvider(), Optional.of(Direction.OUT), traversalPromiseQueryBuilderFactory));
    }

    @Test
    public void SingleTraversalPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Collections.<IdPromise>emptyList(),
                Arrays.asList(new TraversalPromise("roman", __.has("age", P.eq(31)))),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    @Test
    public void MultipleTraversalPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Collections.<IdPromise>emptyList(),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    @Test
    public void MultipleTraversalPromiseAppenderWithPredicatesTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Collections.<IdPromise>emptyList(),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    @Test
    public void SingleIdPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666)),
                Collections.<TraversalPromise>emptyList(),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }


    @Test
    public void MultipleIdPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666), new IdPromise(123), new IdPromise(888)),
                Collections.<TraversalPromise>emptyList(),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }


    @Test
    public void MultipleIdPromiseAppenderWithPredicatesTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666), new IdPromise(123), new IdPromise(888)),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    @Test
    public void SingleIdAndSingleTraversalPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666)),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31)))),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    @Test
    public void IdsAndTraversalsPromiseAppenderTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666), new IdPromise(123), new IdPromise(888)),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Collections.<TraversalPromise>emptyList(),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }


    @Test
    public void IdsAndTraversalsPromiseAppenderWithPredicatesTest() {
        SearchBuilder searchBuilder = new SearchBuilder();
        PromiseBulkInput input = new PromiseBulkInput(
                Arrays.asList(new IdPromise(666), new IdPromise(123), new IdPromise(888)),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Arrays.asList(
                        new TraversalPromise("roman", __.has("age", P.eq(31))),
                        new TraversalPromise("karni", __.or(__.has("age", P.eq(31)), __.has("name", P.eq("gilad")), __.not(__.has("hair")))),
                        new TraversalPromise("doron", __.and(__.has("age", P.eq(25)), __.has("isStudying", P.eq(true))))),
                Arrays.asList("edge1"),
                searchBuilder);

        this.queryAppender.append(input);

        String query = searchBuilder.getSearchRequest(new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", "test").put("client.transport.sniff", true).build())).toString();
        int x = 5;
    }

    //region Fields
    private QueryAppender<PromiseBulkInput> queryAppender;
    //endregion


    public class SimpleSchemaProvider implements GraphElementSchemaProvider {

        @Override
        public Optional<GraphVertexSchema> getVertexSchema(String type) {
            return Optional.of(new GraphVertexSchema() {
                @Override
                public String getType() {
                    return "vertex";
                }

                @Override
                public Optional<GraphElementRouting> getRouting() {
                    return Optional.empty();
                }

                @Override
                public Iterable<String> getIndices() {
                    return Arrays.asList("standard");
                }
            });
        }

        @Override
        public Optional<GraphEdgeSchema> getEdgeSchema(String type, Optional<String> sourceType, Optional<String> destinationType) {
            return Optional.of((GraphEdgeSchema)StreamSupport.stream(getEdgeSchemas(type).get().spliterator(), false).findFirst().get());
        }

        @Override
        public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type) {
            switch (type) {
                case "edge1": return Optional.of(Arrays.asList(getTypedEdgeSchema("edge1")));
                case "edge2": return Optional.of(Arrays.asList(getTypedEdgeSchema("edge2")));
            }

            return Optional.empty();
        }

        @Override
        public Iterable<String> getVertexTypes() {
            return Arrays.asList("vertex");
        }

        @Override
        public Iterable<String> getEdgeTypes() {
            return Arrays.asList("edge1", "edge2");
        }

        //region Private Methods
        public GraphEdgeSchema getTypedEdgeSchema(String type) {
            return new GraphEdgeSchema() {
                @Override
                public Optional<End> getSource() {
                    return Optional.of(new End() {
                        @Override
                        public String getIdField() {
                            return "sourceId";
                        }

                        @Override
                        public Optional<String> getType() {
                            return Optional.of("vertex");
                        }

                        @Override
                        public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                            return Optional.of(new PrefixedEdgeRedundancy("source."));
                        }
                    });
                }

                @Override
                public Optional<End> getDestination() {
                    return Optional.of(new End() {
                        @Override
                        public String getIdField() {
                            return "destinationId";
                        }

                        @Override
                        public Optional<String> getType() {
                            return Optional.of("vertex");
                        }

                        @Override
                        public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                            return Optional.of(new PrefixedEdgeRedundancy("destination."));
                        }
                    });
                }

                @Override
                public Optional<Direction> getDirection() {
                    return Optional.of(new Direction() {
                        @Override
                        public String getField() {
                            return "direction";
                        }

                        @Override
                        public Object getInValue() {
                            return "in";
                        }

                        @Override
                        public Object getOutValue() {
                            return "out";
                        }
                    });
                }

                @Override
                public String getType() {
                    return type;
                }

                @Override
                public Optional<GraphElementRouting> getRouting() {
                    return Optional.empty();
                }

                @Override
                public Iterable<String> getIndices() {
                    return Arrays.asList("standard");
                }
            };
        }
        //endregion
    }
}
