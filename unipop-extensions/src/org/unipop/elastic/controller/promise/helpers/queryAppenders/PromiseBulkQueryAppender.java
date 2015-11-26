package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/24/2015.
 */
public class PromiseBulkQueryAppender extends GraphQueryAppenderBase<PromiseBulkInput> {
    //region Constructor
    public PromiseBulkQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryAppender<PromiseBulkInput> innerPromiseAppender) {
        super(graph, schemaProvider, direction);

        this.innerPromiseAppender = innerPromiseAppender;
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseBulkInput input) {
        return true;
    }

    @Override
    public boolean append(PromiseBulkInput input) {
        boolean appendedSuccesfully = false;

        if (this.innerPromiseAppender.canAppend(input)) {
            appendedSuccesfully = this.innerPromiseAppender.append(input) || appendedSuccesfully;
        }

        if (appendedSuccesfully) {
            for(GraphEdgeSchema edgeSchema : getAllEdgeSchemasFromTypes(input.getTypesToQuery())) {

                input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                        .bool().must(PromiseStringConstants.PROMISES_AND_TYPES_FILTER).bool("typesFilter")
                        .should().bool(edgeSchema.getType()).must()
                        .term(edgeSchema.getType() + "Type", "_type", edgeSchema.getType());

                // in case of dual edge schema, add to each type the relevant direction (unless it's BOTH)
                if (edgeSchema.getDirection().isPresent()) {
                    if (getDirection().isPresent() && getDirection().get() != Direction.BOTH) {
                        input.getSearchBuilder().getQueryBuilder().bool(edgeSchema.getType() + "Direction").should()
                                .term(edgeSchema.getType() + "DirectionTerm", edgeSchema.getDirection().get().getField(),
                                        getDirection().get() == Direction.OUT ?
                                            edgeSchema.getDirection().get().getOutValue() :
                                            edgeSchema.getDirection().get().getInValue());
                    }
                }
            }
        }

        return appendedSuccesfully;
    }
    //endregion

    //region Private Methods
    private Iterable<GraphEdgeSchema> getAllEdgeSchemasFromTypes(Iterable<String> edgeTypes) {
        return StreamSupport.stream(edgeTypes.spliterator(), false)
                .<GraphEdgeSchema>flatMap(typeToQuery -> this.getSchemaProvider().getEdgeSchemas(typeToQuery).isPresent() ?
                        StreamSupport.stream(this.getSchemaProvider().getEdgeSchemas(typeToQuery).get().spliterator(), false) :
                        Stream.empty())
                .collect(Collectors.toList());
    }
    //endregion

    //region Fields
    private QueryAppender<PromiseBulkInput> innerPromiseAppender;
    //endregion
}
