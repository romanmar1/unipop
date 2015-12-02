package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.unipop.elastic.promise.PromiseEdgeTest;
import org.unipop.elastic.promise.PromiseSimilarityEdgeTest;
import org.unipop.elastic.promise.PromiseVertexTest;

/**
 * Created by Roman on 11/17/2015.
 */
public class CustomTestSuite extends AbstractGremlinSuite {
    //private static final Class<?>[] allTests = new Class[]{PromiseEdgeTest.class };
    private static final Class<?>[] allTests = new Class[]{PromiseSimilarityEdgeTest.class };

    public CustomTestSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, (Class[]) null, false, TraversalEngine.Type.STANDARD);
    }
}
