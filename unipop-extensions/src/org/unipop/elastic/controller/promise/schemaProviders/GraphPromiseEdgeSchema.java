package org.unipop.elastic.controller.promise.schemaProviders;

import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

import java.util.Optional;

/**
 * Created by Karni on 11/30/2015.
 */
public interface GraphPromiseEdgeSchema extends GraphEdgeSchema {
    public interface Property {
        String getName();
    }

    Iterable<Property> getProperties();
}
