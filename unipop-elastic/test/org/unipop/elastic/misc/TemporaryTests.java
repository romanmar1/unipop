package org.unipop.elastic.misc;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.unipop.elastic.ElasticGraphProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.junit.Assert.assertEquals;

public class TemporaryTests extends AbstractGremlinTest {

    public TemporaryTests() throws InterruptedException, ExecutionException, IOException {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_Drop() throws Exception {
        GraphTraversal traversal =  g.V().and(has("age", P.gt(27)), outE().count().is(P.gte(2l))).values("name");
        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX2X_inE() {
        GraphTraversal traversal = g.V().out().out().valueMap();
        //GraphTraversal traversal = g.V().out().out().valueMap();

//        int counter = 0;
//
//        while(traversal.hasNext()) {
//            ++counter;
//            Vertex vertex = (Vertex)traversal.next();
//            Assert.assertTrue(vertex.value("name").equals("lop") || vertex.value("name").equals("ripple"));
//        }
//
//        Assert.assertEquals(2L, (long)counter);
//        Assert.assertFalse(traversal.hasNext());

        check(traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_repeat() {
        GraphTraversal<Vertex, Vertex> traversal = g.V().repeat(out()).times(1);
        check(traversal);
    }

    private void check(Traversal traversal) {
        System.out.println("pre-strategy:" + traversal);
        traversal.hasNext();
        System.out.println("post-strategy:" + traversal);

        //traversal.profile().cap(TraversalMetrics.METRICS_KEY);

        while(traversal.hasNext())
            System.out.println(traversal.next());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_unionXrepeatXoutX_timesX2X__outX_name() {
        final Traversal<Vertex, String> traversal = get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(convertToVertexId("marko"));
        printTraversalForm(traversal);
//        checkResults(new HashMap<String, Long>() {{
//            put("lop", 2l);
//            put("ripple", 1l);
//            put("josh", 1l);
//            put("vadas", 1l);
//        }}, traversal);
        check(traversal);
    }
    public Traversal<Vertex, String> get_g_VX1X_unionXrepeatXoutX_timesX2X__outX_name(final Object v1Id) {
        return g.V(v1Id).union(repeat(out()).times(2), out()).values("name");
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_V_valueMap() {
        final Traversal<Vertex, Map<String, List>> traversal = g.V().valueMap();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            assertEquals(2, values.size());
            if (name.equals("marko")) {
                assertEquals(29, values.get("age").get(0));
            } else if (name.equals("josh")) {
                assertEquals(32, values.get("age").get(0));
            } else if (name.equals("peter")) {
                assertEquals(35, values.get("age").get(0));
            } else if (name.equals("vadas")) {
                assertEquals(27, values.get("age").get(0));
            } else if (name.equals("lop")) {
                assertEquals("java", values.get("lang").get(0));
            } else if (name.equals("ripple")) {
                assertEquals("java", values.get("lang").get(0));
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        assertEquals(6, counter);
    }
}
