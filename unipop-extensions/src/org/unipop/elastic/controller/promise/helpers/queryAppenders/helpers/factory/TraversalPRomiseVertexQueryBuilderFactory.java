package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.TraversalQueryTranslator;

/**
 * Created by Roman on 12/1/2015.
 */
public class TraversalPromiseVertexQueryBuilderFactory implements QueryBuilderFactory<TraversalPromiseVertexInput> {
    //region QueryBuilderFactory Implementation
    @Override
    public QueryBuilder getPromiseQueryBuilder(TraversalPromiseVertexInput input) {
        QueryBuilder traversalPromiseQueryBuilder = new QueryBuilder().query().filtered().filter();

        TraversalQueryTranslator traversalQueryTranslator = new TraversalQueryTranslator(input.getSearchBuilder(), traversalPromiseQueryBuilder);
        traversalQueryTranslator.visit(input.getTraversalPromise().getTraversal());

        return traversalPromiseQueryBuilder;
    }
    //endregion
}
