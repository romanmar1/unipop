package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

import java.util.Set;

/**
 * Created by Roman on 11/26/2015.
 */
public class IdPromiseEdgeInput {
    //region Constructor
    public IdPromiseEdgeInput(Iterable<IdPromise> idPromises, Iterable<GraphEdgeSchema> edgeSchemas) {
        this.idPromises = idPromises;
        this.edgeSchemas = edgeSchemas;
    }
    //endregion

    //region Properties
    public Iterable<IdPromise> getIdPromises() {
        return this.idPromises;
    }

    public Iterable<GraphEdgeSchema> getEdgeSchemas() {
        return edgeSchemas;
    }
    //endregion

    //region Fields
    Iterable<IdPromise> idPromises;
    Iterable<GraphEdgeSchema> edgeSchemas;
    //endregion
}
