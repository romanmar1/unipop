package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.unipop.elastic.controller.promise.Promise;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/17/2015.
 */
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
        int x = 5;
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void promisePredicatesStrategy() {
        Traversal traversal = g.V().outE().out().has("bla",2).has("anotherBla", P.within((new ArrayList<Integer>()).add(666)));

        printTraversalForm(traversal);
        int x = 5;
    }
}
