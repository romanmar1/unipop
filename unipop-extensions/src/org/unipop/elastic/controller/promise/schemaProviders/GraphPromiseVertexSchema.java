package org.unipop.elastic.controller.promise.schemaProviders;

import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;

/**
 * Created by Roman on 12/1/2015.
 */
public interface GraphPromiseVertexSchema extends GraphVertexSchema {
    interface PromiseSimilarity {
        Iterable<String> getSimilarityProperties();
    }

    PromiseSimilarity getSimilarity();
}
