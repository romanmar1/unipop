package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/23/2015.
 */
public class SingularTraversalPromiseQueryAppender extends GraphQueryAppenderBase<PromiseTypesBulkInput<TraversalPromise>> {
    //region Constructor
    public SingularTraversalPromiseQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseTypesBulkInput<TraversalPromise> input) {

        return true;
    }

    @Override
    public boolean append(PromiseTypesBulkInput<TraversalPromise> input) {
        return false;
    }
    //endregion
}
