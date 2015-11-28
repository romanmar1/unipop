package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Created by Roman on 11/28/2015.
 */
public interface TraversalIdProvider<T> {
    public T getId(Traversal traversal);
}
