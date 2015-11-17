package org.unipop.elastic.promise;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Test;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseTests {
    @Test
    public void simplePromiseTraversalToSearchBuilder() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.has("prop3", "val3")), __.has("prop4", "val4"));

        TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator();
        SearchBuilder searchBuilder = new SearchBuilder();
        traversalQueryTranslator.applyTraversal(searchBuilder, searchBuilder.getQueryBuilder().query().filtered().filter(), traversal);

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }

    @Test
    public void promiseTraversalToSearchBuilderWithTraversalFilterStep() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.has("someProperty")));

        TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator();
        SearchBuilder searchBuilder = new SearchBuilder();
        traversalQueryTranslator.applyTraversal(searchBuilder, searchBuilder.getQueryBuilder().query().filtered().filter(), traversal);

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }

    @Test
    public void promiseTraversalToSearchBuilderWithNotStep() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.not(__.has("someProperty"))));

        TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator();
        SearchBuilder searchBuilder = new SearchBuilder();
        traversalQueryTranslator.applyTraversal(searchBuilder, searchBuilder.getQueryBuilder().query().filtered().filter(), traversal);

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }
}
