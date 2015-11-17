package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Created by Roman on 11/17/2015.
 */
@RunWith(GremlinProcessRunner.class)
public class PromiseVertexTest extends AbstractGremlinTest {
    @Test
    public void test1() {
        Iterable<Vertex> vertices = g.V().toList();
        int x = 5;
    }

}
