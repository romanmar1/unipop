package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */
public abstract class DualPromiseQueryAppenderBase extends GraphQueryAppenderBase<PromiseBulkInput> {
    //region Constructor
    public DualPromiseQueryAppenderBase(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllDualEdgeSchemasFromTypes(input.getTypesToQuery());
        if (StreamSupport.stream(edgeSchemas.spliterator(), false).count() == 0) {
            return false;
        }

        return StreamSupport.stream(edgeSchemas.spliterator(), false).count() > 0;
    }
    //endregion

    //region Protected Methods
    protected Iterable<GraphEdgeSchema> getAllDualEdgeSchemasFromTypes(Iterable<String> edgeTypes) {
        return StreamSupport.stream(edgeTypes.spliterator(), false)
                .<GraphEdgeSchema>flatMap(typeToQuery -> this.getSchemaProvider().getEdgeSchemas(typeToQuery).isPresent() ?
                        StreamSupport.stream(this.getSchemaProvider().getEdgeSchemas(typeToQuery).get().spliterator(), false) :
                        Stream.empty())
                .filter(edgeSchema -> edgeSchema.getDirection().isPresent())
                .collect(Collectors.toList());
    }


    //endregion
}
