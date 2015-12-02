package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Roman on 11/27/2015.
 */
public class DualPromiseDirectionQueryAppender extends DualPromiseQueryAppenderBase{
    //region Constructor
    public DualPromiseDirectionQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction) {
        super(graph, schemaProvider, direction);
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllDualEdgeSchemasFromTypes(input.getTypesToQuery());
        Map<String, Seq<String>> directionFields = Seq.seq(edgeSchemas).map(edgeSchema -> edgeSchema.getDirection().get())
                .collect(Collectors.toMap(direction -> direction.getField(),
                        direction -> getDirection().get() == Direction.IN ? Seq.of(direction.getInValue().toString()) :
                                getDirection().get() == Direction.OUT ? Seq.of(direction.getOutValue().toString()) :
                                        Seq.<String>empty(),
                        (seq1, seq2) -> seq1.concat(seq2).distinct()));

        for(Map.Entry<String, Seq<String>> directionEntry : directionFields.entrySet()) {
            Iterable<String> directionValues = directionEntry.getValue().toList();
            if (Seq.seq(directionValues).count() > 0) {
                if (directionFields.size() == 1) {
                    input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                            .bool(PromiseStrings.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .terms(directionEntry.getKey(), directionValues);
                } else {
                    input.getSearchBuilder().getQueryBuilder().seekRoot().query().filtered().filter()
                            .bool(PromiseStrings.PROMISES_TYPES_DIRECTIONS_FILTER).must()
                            .bool(PromiseStrings.DIRECTIONS_FILTER).should()
                            .terms(directionEntry.getKey(), directionValues);
                }
            }
        }

        return directionFields.size() > 0;
    }
    //endregion
}
