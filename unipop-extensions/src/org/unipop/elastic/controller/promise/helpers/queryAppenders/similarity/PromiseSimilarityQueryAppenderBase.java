package org.unipop.elastic.controller.promise.helpers.queryAppenders.similarity;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 12/1/2015.
 */
public abstract class PromiseSimilarityQueryAppenderBase extends GraphQueryAppenderBase<PromiseBulkInput> {
    //region Constructor
    public PromiseSimilarityQueryAppenderBase(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region Protected Methods

    //endregion
}
