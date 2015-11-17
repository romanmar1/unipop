package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.elastic.promise.PromiseGraphProvider;
import org.unipop.structure.UniGraph;

/**
 * Created by Roman on 11/17/2015.
 */
@RunWith(CustomTestSuite.class)
@GraphProviderClass(provider = PromiseGraphProvider.class, graph = UniGraph.class)
public class PromiseGraphCustomSuite {
}
