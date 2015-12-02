package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.jooq.lambda.Seq;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.unipop.elastic.controller.promise.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Roman on 11/17/2015.
 */
@SuppressWarnings("Duplicates")
@RunWith(GremlinProcessRunner.class)
public class PromiseEdgeTest extends AbstractGremlinTest {
   /* @Test
    public void g_E_EmptyGraph() {
        List<Edge> edges = g.E().toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_E() {
        List<Edge> edges = g.E().toList();
        Assert.assertEquals(6, edges.size());

        edges.forEach(edge -> Assert.assertEquals(PromiseEdge.class, edge.getClass()));
        edges.forEach(edge -> Assert.assertNotNull(edge.inVertex()));
        edges.forEach(edge -> Assert.assertNotNull(edge.outVertex()));
        edges.stream().flatMap(edge -> Arrays.asList(edge.inVertex(), edge.outVertex()).stream())
                .forEach(vertex -> {
                    Assert.assertEquals(PromiseVertex.class, vertex.getClass());
                    Assert.assertEquals(vertex.id(), ((PromiseVertex)vertex).getPromise().getId());
                });

    }*/

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outE() {
        List<Edge> edges = g.V().outE().toList();

        Assert.assertEquals(6, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_inE() {
        List<Edge> edges = g.V().inE().toList();

        Assert.assertEquals(6, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_bothE() {
        List<Edge> edges = g.V().bothE().toList();

        Assert.assertEquals(12, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });

        Map<String, List<Edge>> edgeGroupBy = Seq.seq(edges).groupBy(edge -> edge.id().toString());
        Assert.assertEquals(6, edgeGroupBy.size());
        edgeGroupBy.entrySet().forEach(entry -> Assert.assertEquals(2, entry.getValue().size()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outE_hasXweight_eq_1_0X() {
        List<Edge> edges = g.V().outE().has("weight", P.eq(1.0)).toList();

        Assert.assertEquals(2, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outE_hasXweight_neq_1_0X() {
        List<Edge> edges = g.V().outE().has("weight", P.neq(1.0)).toList();

        Assert.assertEquals(4, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outEXknowsX() {
        List<Edge> edges = g.V().outE("knows").toList();

        Assert.assertEquals(2, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outEXcreatedX() {
        List<Edge> edges = g.V().outE("created").toList();

        Assert.assertEquals(4, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Seq.seq(Arrays.asList(edge.inVertex(), edge.outVertex())).cast(PromiseVertex.class).forEach(vertex -> {
                Assert.assertEquals(IdPromise.class, vertex.getPromise().getClass());
            });

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_inE_hasXpredicates_promise_eq_promiseXmarko_hasXname_markoXXX() {
        List<Edge> edges = g.V().inE().has("predicates_promise", P.eq(new TraversalPromise("marko", __.has("name", "marko")))).toList();

        Assert.assertEquals(3, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());

            Assert.assertEquals(IdPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outE_hasXpredicates_promise_eq_promiseXmarko_hasXname_markoXXX() {
        List<Edge> edges = g.V().outE().has("predicates_promise", P.eq(new TraversalPromise("marko", __.has("name", "marko")))).toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_outE() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", "marko")))).outE().toList();

        Assert.assertEquals(3, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());

            Assert.assertEquals(IdPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_outE_hasXpredicates_promise_eq_promiseXjava_hasXlang_javaXXX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", "marko"))))
                .outE().has("predicates_promise", P.eq(new TraversalPromise("java", __.has("lang", "java")))).toList();

        Assert.assertEquals(1, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());

            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());

            Assert.assertEquals("marko", edge.outVertex().id().toString());
            Assert.assertEquals("java", edge.inVertex().id().toString());

            Assert.assertEquals(1L, edge.property("count").value());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_inE_hasXpredicates_promise_eq_promiseXjava_hasXlang_javaXXX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", "marko"))))
                .inE().has("predicates_promise", P.eq(new TraversalPromise("java", __.has("lang", "java")))).toList();

        Assert.assertEquals(0, edges.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXjava_hasXlang_javaXXX_bothE() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("java", __.has("lang", "java")))).bothE().toList();

        Assert.assertEquals(3, edges.size());
        Seq.seq(edges).forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());

            Assert.assertEquals(IdPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());

            Assert.assertEquals("java", edge.outVertex().id().toString());
        });

        List<Long> counts = Seq.seq(edges).map(edge -> (Long)edge.property("count").value()).sorted().toList();
        Assert.assertEquals((Long)1L, (Long)counts.get(0));
        Assert.assertEquals((Long)1L, (Long)counts.get(1));
        Assert.assertEquals((Long)2L, (Long)counts.get(2));
    }

//    @Test
//    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
//    public void promisePredicatesStrategy() {
//        Traversal traversal = g.V().outE().out().has("promise", P.eq(new TraversalPromise("promise1", __.has("bla",2)))).has("promise", P.within(new TraversalPromise("promise2", __.has("name", "marko"))));
//
//        printTraversalForm(traversal);
//        int x = 5;
//    }
}
