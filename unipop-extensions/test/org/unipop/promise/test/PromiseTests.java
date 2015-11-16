package org.unipop.promise.test;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Test;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.extensions.controller.promise.Promise;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseTests {
    @Test
    public void simplePromiseTraversalToSearchBuilder() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.has("prop3", "val3")), __.has("prop4", "val4"));

        SearchBuilder searchBuilder = new SearchBuilder();
        Promise promise = new Promise(traversal);
        promise.addToQuery(searchBuilder, searchBuilder.getQueryBuilder());

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }

    @Test
    public void promiseTraversalToSearchBuilderWithTraversalFilterStep() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.has("someProperty")));

        SearchBuilder searchBuilder = new SearchBuilder();
        Promise promise = new Promise(traversal);
        promise.addToQuery(searchBuilder, searchBuilder.getQueryBuilder());

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }

    @Test
    public void promiseTraversalToSearchBuilderWithNotStep() {
        Traversal traversal = __.or(__.has("prop1", "val1"), __.and(__.has("prop2", P.gt(2)), __.not(__.has("someProperty"))));

        SearchBuilder searchBuilder = new SearchBuilder();
        Promise promise = new Promise(traversal);
        promise.addToQuery(searchBuilder, searchBuilder.getQueryBuilder());

        String query = searchBuilder.getQueryBuilder().getQuery().toString();
        int x = 5;
    }
}
