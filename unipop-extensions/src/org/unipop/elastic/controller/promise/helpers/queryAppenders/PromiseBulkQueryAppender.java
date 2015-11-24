package org.unipop.elastic.controller.promise.helpers.queryAppenders;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.GraphQueryAppenderBase;
import org.unipop.elastic.controller.schema.helpers.queryAppenders.QueryAppender;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
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
            QueryAppender<PromiseTypesBulkInput<IdPromise>> idPromiseAppender,
            QueryAppender<PromiseTypesBulkInput<TraversalPromise>> traversalPromiseAppender) {
        super(graph, schemaProvider, direction);

        this.idPromiseAppender = idPromiseAppender;
        this.traversalPromiseAppender = traversalPromiseAppender;
    }
    //endregion

    //region GraphQueryAppenderBase Implementation
    @Override
    public boolean canAppend(PromiseBulkInput input) {
        return true;
    }

    @Override
    public boolean append(PromiseBulkInput input) {
        Map<String, List<IdPromise>> labelMap = getPartionedByLabelIdPromiseMap(input.getIdPromisesBulk());

        boolean appendedSuccesfully = false;
        for(String edgeLabel : input.getTypesToQuery()) {
            for(Map.Entry<String, List<IdPromise>> entry : labelMap.entrySet()) {
                PromiseTypesBulkInput<IdPromise> idPromiseAppenderInput = new PromiseTypesBulkInput<>(
                        entry.getValue(),
                        input.getTraversalPromisesPredicates(),
                        Arrays.asList(edgeLabel),
                        input.getSearchBuilder());

                if (this.idPromiseAppender.canAppend(idPromiseAppenderInput)) {
                    appendedSuccesfully = this.idPromiseAppender.append(idPromiseAppenderInput) || appendedSuccesfully;
                }
            }
        }

        PromiseTypesBulkInput<TraversalPromise> traversalPromiseAppenderInput = new PromiseTypesBulkInput<TraversalPromise>(
                input.getTraversalPromisesBulk(),
                input.getTraversalPromisesPredicates(),
                input.getTypesToQuery(),
                input.getSearchBuilder());

        if (this.traversalPromiseAppender.canAppend(traversalPromiseAppenderInput)) {
            appendedSuccesfully = this.traversalPromiseAppender.append(traversalPromiseAppenderInput) || appendedSuccesfully;
        }

        return  appendedSuccesfully;
    }
    //endregion

    //region Private Methods
    private Map<String, List<IdPromise>> getPartionedByLabelIdPromiseMap(Iterable<IdPromise> idPromises) {
        return StreamSupport.stream(idPromises.spliterator(), false)
                .collect(Collectors.toMap(
                        idPromise -> idPromise.getLabel(),
                        idPromise -> new ArrayList<>(Arrays.asList(idPromise)),
                        (list1, list2) -> {
                            if (list1.size() > list2.size()) {
                                list1.addAll(list2);
                                return list1;
                            } else {
                                list2.addAll(list1);
                                return list2;
                            }
                        }));
    }
    //endregion

    //region Fields
    private QueryAppender<PromiseTypesBulkInput<IdPromise>> idPromiseAppender;
    private QueryAppender<PromiseTypesBulkInput<TraversalPromise>> traversalPromiseAppender;
    //endregion
}
