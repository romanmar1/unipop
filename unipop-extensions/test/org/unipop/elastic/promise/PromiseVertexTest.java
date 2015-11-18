package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.unipop.elastic.controller.promise.Promise;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.promise.TraversalPromise;

import java.util.List;

/**
 * Created by Roman on 11/17/2015.
 */
@RunWith(GremlinProcessRunner.class)
public class PromiseVertexTest extends AbstractGremlinTest {
    @Test
    public void g_V_EmptyGraph() {
        List<Vertex> vertices = g.V().toList();

        Assert.assertEquals(0, vertices.size());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V() {
        List<Vertex> vertices = g.V().toList();
        Assert.assertEquals(6, vertices.size());

        vertices.forEach(vertex -> Assert.assertEquals(PromiseVertex.class, vertex.getClass()));
        vertices.forEach(vertex -> Assert.assertNotNull(((PromiseVertex)vertex).getPromise()));
        vertices.forEach(vertex -> Assert.assertEquals(vertex.id(), ((PromiseVertex)vertex).getPromise().getId()));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX() {
        List<Vertex> vertices = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", "marko")))).toList();

        Assert.assertEquals(1, vertices.size());
        vertices.forEach(vertex -> Assert.assertEquals(PromiseVertex.class, vertex.getClass()));

        Assert.assertEquals("marko", vertices.get(0).id());
        Assert.assertEquals("marko", ((PromiseVertex)vertices.get(0)).getPromise().getId());

        Traversal.Admin promiseTraversal = ((TraversalPromise)((PromiseVertex)vertices.get(0)).getPromise()).getTraversal().asAdmin();
        Assert.assertEquals(1, promiseTraversal.getSteps().size());
        Assert.assertEquals(HasStep.class, promiseTraversal.getSteps().get(0).getClass());
        Assert.assertEquals(1, ((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().size());
        Assert.assertEquals("name", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getKey());
        Assert.assertEquals("marko", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getValue());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_withinXpromiseXmarko_hasXname_markoXX_promiseXvadas_hasXname_vadasXXXX() {
        List<Vertex> vertices = g.V().has("promise", P.within(
                new TraversalPromise("marko", __.has("name", "marko")),
                new TraversalPromise("vadas", __.has("name", "vadas")))).toList();

        Assert.assertEquals(2, vertices.size());
        vertices.forEach(vertex -> Assert.assertEquals(PromiseVertex.class, vertex.getClass()));

        Assert.assertEquals("marko", vertices.get(0).id());
        Assert.assertEquals("marko", ((PromiseVertex)vertices.get(0)).getPromise().getId());

        Traversal.Admin promiseTraversal = ((TraversalPromise)((PromiseVertex)vertices.get(0)).getPromise()).getTraversal().asAdmin();
        Assert.assertEquals(1, promiseTraversal.getSteps().size());
        Assert.assertEquals(HasStep.class, promiseTraversal.getSteps().get(0).getClass());
        Assert.assertEquals(1, ((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().size());
        Assert.assertEquals("name", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getKey());
        Assert.assertEquals("marko", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getValue());

        Assert.assertEquals("vadas", vertices.get(1).id());
        Assert.assertEquals("vadas", ((PromiseVertex)vertices.get(1)).getPromise().getId());

        promiseTraversal = ((TraversalPromise)((PromiseVertex)vertices.get(1)).getPromise()).getTraversal().asAdmin();
        Assert.assertEquals(1, promiseTraversal.getSteps().size());
        Assert.assertEquals(HasStep.class, promiseTraversal.getSteps().get(0).getClass());
        Assert.assertEquals(1, ((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().size());
        Assert.assertEquals("name", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getKey());
        Assert.assertEquals("vadas", ((HasContainer)((HasStep)promiseTraversal.getSteps().get(0)).getHasContainers().get(0)).getValue());
    }
}
