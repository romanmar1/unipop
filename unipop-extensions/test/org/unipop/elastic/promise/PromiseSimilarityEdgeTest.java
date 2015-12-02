package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.jooq.lambda.Seq;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.unipop.elastic.controller.promise.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Roman on 12/2/2015.
 */
@SuppressWarnings("Duplicates")
@RunWith(GremlinProcessRunner.class)
public class PromiseSimilarityEdgeTest extends AbstractGremlinTest {
    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_outEXsameAsX() {
        List<Edge> edges = g.V("1").outE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());

        Assert.assertEquals(IdPromise.class, ((PromiseVertex)edges.get(0).outVertex()).getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edges.get(0).inVertex()).getPromise().getClass());

        Assert.assertEquals("1", edges.get(0).outVertex().id());

        Traversal traversal = ((TraversalPromise)((PromiseVertex)edges.get(0).inVertex()).getPromise()).getTraversal();
        Traversal expectedTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.eq(29)));
        Assert.assertTrue(expectedTraversal.equals(traversal));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_inEXsameAsX() {
        List<Edge> edges = g.V("1").inE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());

        Assert.assertEquals(IdPromise.class, ((PromiseVertex)edges.get(0).inVertex()).getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edges.get(0).outVertex()).getPromise().getClass());

        Assert.assertEquals("1", edges.get(0).inVertex().id());

        Traversal traversal = ((TraversalPromise)((PromiseVertex)edges.get(0).outVertex()).getPromise()).getTraversal();
        Traversal expectedTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.eq(29)));
        Assert.assertTrue(expectedTraversal.equals(traversal));
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX1X_bothEXsameAsX() {
        List<Edge> edges = g.V("1").bothE("sameAs").toList();

        Traversal expectedTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.eq(29)));

        Assert.assertEquals(2, edges.size());

        edges.forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());

            Promise inPromise =  ((PromiseVertex)edge.inVertex()).getPromise();
            Promise outPromise =  ((PromiseVertex)edge.outVertex()).getPromise();

            Assert.assertTrue((inPromise.getClass() == IdPromise.class && outPromise.getClass() == TraversalPromise.class) ||
                    (inPromise.getClass() == TraversalPromise.class && outPromise.getClass() == IdPromise.class));

            Assert.assertTrue(edge.inVertex().id().equals("1") || edge.outVertex().id().equals("1"));
            Assert.assertTrue(inPromise.getClass() == TraversalPromise.class ? expectedTraversal.equals(((TraversalPromise)inPromise).getTraversal()) :
                    expectedTraversal.equals(((TraversalPromise)outPromise).getTraversal()));
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_outEXsameAsX() {
        List<Edge> edges = g.V().outE("sameAs").toList();

        List<Traversal> expectedTraversals = Arrays.asList(
                __.or(__.has("name", P.eq("marko")), __.has("age", P.eq(29))),
                __.or(__.has("name", P.eq("vadas")), __.has("age", P.eq(27))),
                __.or(__.has("name", P.eq("lop")), __.has("lang", P.eq("java"))),
                __.or(__.has("name", P.eq("josh")), __.has("age", P.eq(32))),
                __.or(__.has("name", P.eq("ripple")), __.has("lang", P.eq("java"))),
                __.or(__.has("name", P.eq("peter")), __.has("age", P.eq(35))));

        Assert.assertEquals(6, edges.size());
        edges.forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());

            Assert.assertEquals(IdPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());

            Traversal traversal = ((TraversalPromise)((PromiseVertex)edge.inVertex()).getPromise()).getTraversal();
            Assert.assertEquals(1, Seq.seq(expectedTraversals).filter(expectedTraversal -> expectedTraversal.equals(traversal)).count());
        });

    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_inEXsameAsX() {
        List<Edge> edges = g.V().inE("sameAs").toList();

        List<Traversal> expectedTraversals = Arrays.asList(
                __.or(__.has("name", P.eq("marko")), __.has("age", P.eq(29))),
                __.or(__.has("name", P.eq("vadas")), __.has("age", P.eq(27))),
                __.or(__.has("name", P.eq("lop")), __.has("lang", P.eq("java"))),
                __.or(__.has("name", P.eq("josh")), __.has("age", P.eq(32))),
                __.or(__.has("name", P.eq("ripple")), __.has("lang", P.eq("java"))),
                __.or(__.has("name", P.eq("peter")), __.has("age", P.eq(35))));

        Assert.assertEquals(6, edges.size());
        edges.forEach(edge -> {
            Assert.assertEquals(PromiseEdge.class, edge.getClass());
            Assert.assertEquals(PromiseVertex.class, edge.outVertex().getClass());
            Assert.assertEquals(PromiseVertex.class, edge.inVertex().getClass());

            Assert.assertEquals(IdPromise.class, ((PromiseVertex)edge.inVertex()).getPromise().getClass());
            Assert.assertEquals(TraversalPromise.class, ((PromiseVertex)edge.outVertex()).getPromise().getClass());

            Traversal traversal = ((TraversalPromise)((PromiseVertex)edge.outVertex()).getPromise()).getTraversal();
            Assert.assertEquals(1, Seq.seq(expectedTraversals).filter(expectedTraversal -> expectedTraversal.equals(traversal)).count());
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_outEXsameAsX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", P.eq("marko"))))).outE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());

        PromiseVertex outVertex = (PromiseVertex)edges.get(0).outVertex();
        PromiseVertex inVertex = (PromiseVertex)edges.get(0).inVertex();

        Assert.assertEquals("marko", outVertex.id());
        Assert.assertEquals("marko", inVertex.id());

        Assert.assertEquals(TraversalPromise.class, outVertex.getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, inVertex.getPromise().getClass());

        Traversal expectedOutTraversal = __.has("name", P.eq("marko"));
        Traversal expectedInTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.within(Arrays.asList("29"))));

        Assert.assertEquals(expectedInTraversal, ((TraversalPromise)inVertex.getPromise()).getTraversal());
        Assert.assertEquals(expectedOutTraversal, ((TraversalPromise)outVertex.getPromise()).getTraversal());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_inEXsameAsX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", P.eq("marko"))))).inE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());

        PromiseVertex outVertex = (PromiseVertex)edges.get(0).outVertex();
        PromiseVertex inVertex = (PromiseVertex)edges.get(0).inVertex();

        Assert.assertEquals("marko", outVertex.id());
        Assert.assertEquals("marko", inVertex.id());

        Assert.assertEquals(TraversalPromise.class, outVertex.getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, inVertex.getPromise().getClass());

        Traversal expectedInTraversal = __.has("name", P.eq("marko"));
        Traversal expectedOutTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.within(Arrays.asList("29"))));

        Assert.assertEquals(expectedInTraversal, ((TraversalPromise)inVertex.getPromise()).getTraversal());
        Assert.assertEquals(expectedOutTraversal, ((TraversalPromise)outVertex.getPromise()).getTraversal());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXmarko_hasXname_markoXXX_outEXsameAsX_inV_outEXsameAsX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("marko", __.has("name", P.eq("marko"))))).outE("sameAs").inV().outE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());

        PromiseVertex outVertex = (PromiseVertex)edges.get(0).outVertex();
        PromiseVertex inVertex = (PromiseVertex)edges.get(0).inVertex();

        Assert.assertEquals("marko", outVertex.id());
        Assert.assertEquals("marko", inVertex.id());

        Assert.assertEquals(TraversalPromise.class, outVertex.getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, inVertex.getPromise().getClass());

        Traversal expectedOutTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.within(Arrays.asList("29"))));
        Traversal expectedInTraversal = __.or(__.has("name", P.eq("marko")), __.has("age", P.within(Arrays.asList("29"))));

        Assert.assertEquals(expectedInTraversal, ((TraversalPromise)inVertex.getPromise()).getTraversal());
        Assert.assertEquals(expectedOutTraversal, ((TraversalPromise)outVertex.getPromise()).getTraversal());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_V_hasXpromise_eq_promiseXjava_hasXlang_javaXXX_outEXsameAsX() {
        List<Edge> edges = g.V().has("promise", P.eq(new TraversalPromise("java", __.has("lang", P.eq("java"))))).outE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());

        PromiseVertex outVertex = (PromiseVertex)edges.get(0).outVertex();
        PromiseVertex inVertex = (PromiseVertex)edges.get(0).inVertex();

        Assert.assertEquals("java", outVertex.id());
        Assert.assertEquals("java", inVertex.id());

        Assert.assertEquals(TraversalPromise.class, outVertex.getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, inVertex.getPromise().getClass());

        Traversal expectedOutTraversal = __.has("lang", P.eq("java"));
        Traversal expectedInTraversal = __.or(__.has("lang", P.eq("java")), __.has("name", P.within(Arrays.asList("ripple", "lop"))));

        Assert.assertEquals(expectedInTraversal, ((TraversalPromise)inVertex.getPromise()).getTraversal());
        Assert.assertEquals(expectedOutTraversal, ((TraversalPromise)outVertex.getPromise()).getTraversal());
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void g_VX5X_outEXsameAsX_inV_outEXsameAsX() {
        List<Edge> edges = g.V("5").outE("sameAs").inV().outE("sameAs").toList();

        Assert.assertEquals(1, edges.size());
        Assert.assertEquals(PromiseEdge.class, edges.get(0).getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).inVertex().getClass());
        Assert.assertEquals(PromiseVertex.class, edges.get(0).outVertex().getClass());

        PromiseVertex outVertex = (PromiseVertex)edges.get(0).outVertex();
        PromiseVertex inVertex = (PromiseVertex)edges.get(0).inVertex();

        Assert.assertEquals(TraversalPromise.class, outVertex.getPromise().getClass());
        Assert.assertEquals(TraversalPromise.class, inVertex.getPromise().getClass());

        Traversal expectedOutTraversal = __.or(__.has("name", P.eq("ripple")), __.has("lang", P.eq("java")));
        Traversal expectedInTraversal = __.or(__.has("name", P.within(Arrays.asList("ripple", "lop"))), __.has("lang", P.eq("java")));

        Assert.assertEquals(expectedInTraversal, ((TraversalPromise)inVertex.getPromise()).getTraversal());
        Assert.assertEquals(expectedOutTraversal, ((TraversalPromise)outVertex.getPromise()).getTraversal());
    }
}
