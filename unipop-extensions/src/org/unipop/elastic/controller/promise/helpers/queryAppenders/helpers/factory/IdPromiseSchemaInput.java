package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory;

import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchema;

import java.util.Set;

/**
 * Created by Roman on 11/26/2015.
 */
public class IdPromiseSchemaInput<TSchemaElement extends GraphElementSchema> {
    //region Constructor
    public IdPromiseSchemaInput(Iterable<IdPromise> idPromises, Iterable<TSchemaElement> elementSchemas) {
        this.idPromises = idPromises;
        this.elementSchemas = elementSchemas;
    }
    //endregion

    //region Properties
    public Iterable<IdPromise> getIdPromises() {
        return this.idPromises;
    }

    public Iterable<TSchemaElement> getElementSchemas() {
        return elementSchemas;
    }
    //endregion

    //region Fields
    Iterable<IdPromise> idPromises;
    Iterable<TSchemaElement> elementSchemas;
    //endregion
}
