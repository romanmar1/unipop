package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

/**
 * Created by Roman on 11/26/2015.
 */
public class TraversalPromiseEdgeInput extends TraversalPromiseInput {
    public enum EdgeEnd {
        source,
        destination
    }

    //region Constructor
    public TraversalPromiseEdgeInput(
            TraversalPromise traversalPromise,
            SearchBuilder searchBuilder,
            Iterable<GraphEdgeSchema> edgeSchemas,
            EdgeEnd edgeEnd) {
        super(traversalPromise);
        this.searchBuilder = searchBuilder;
        this.edgeSchemas = edgeSchemas;
        this.edgeEnd = edgeEnd;
    }
    //endregion

    //region Properties
    public SearchBuilder getSearchBuilder() {
        return searchBuilder;
    }

    public Iterable<GraphEdgeSchema> getEdgeSchemas() {
        return edgeSchemas;
    }

    public EdgeEnd getEdgeEnd() {
        return this.edgeEnd;
    }
    //endregion

    //region Fields
    private SearchBuilder searchBuilder;
    private Iterable<GraphEdgeSchema> edgeSchemas;
    private EdgeEnd edgeEnd;
    //endregion
}
