package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.Optional;

/**
 * Created by Roman on 11/27/2015.
 */
public class DualPromiseTypesQueryAppender extends DualPromiseQueryAppenderBase{
    //region Constructor
    public DualPromiseTypesQueryAppender(UniGraph graph, GraphElementSchemaProvider schemaProvider, Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllDualEdgeSchemasFromTypes(input.getTypesToQuery());
        Iterable<String> edgeTypes = Seq.seq(edgeSchemas).map(edgeSchema -> edgeSchema.getType()).distinct().toList();

        input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                .bool(PromiseStrings.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                .terms(PromiseStrings.TYPES_FILTER, "_type", edgeTypes);

        return Seq.seq(edgeTypes).count() > 0;
    }
    //endregion
}
