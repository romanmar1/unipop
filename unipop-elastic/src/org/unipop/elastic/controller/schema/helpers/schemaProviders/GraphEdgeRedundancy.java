package org.unipop.elastic.controller.schema.helpers.schemaProviders;

import java.util.Optional;

/**
 * Created by Karni on 11/23/2015.
 */
public interface GraphEdgeRedundancy {
    Optional<String> getRedundantPropertyName(String propertyName);
}
