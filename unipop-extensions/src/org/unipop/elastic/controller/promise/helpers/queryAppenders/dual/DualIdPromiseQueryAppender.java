package org.unipop.elastic.controller.promise.helpers.queryAppenders.dual;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.PromiseBulkInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.IdPromiseEdgeInput;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.QueryBuilderFactory;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.factory.TraversalPromiseEdgeInput;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by Roman on 11/26/2015.
 */
@SuppressWarnings("Duplicates")
public class DualIdPromiseQueryAppender extends DualPromiseQueryAppenderBase {
    //region Constructor
    public DualIdPromiseQueryAppender(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Optional<Direction> direction,
            QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory,
            QueryBuilderFactory<TraversalPromiseEdgeInput> traversalPromiseQueryBuilderFactory) {
        super(graph, schemaProvider, direction, traversalPromiseQueryBuilderFactory);
        this.idPromiseQueryBuilderFactory = idPromiseQueryBuilderFactory;
    }
    //endregion

    //region DualPromiseQueryAppenderBase Implementation
    @Override
    public boolean append(PromiseBulkInput input) {
        Iterable<GraphEdgeSchema> edgeSchemas = getAllEdgeSchemasFromTypes(input.getTypesToQuery());

        Set<String> sourceIdFields = StreamSupport.stream(edgeSchemas.spliterator(), false)
                .map(edgeSchema -> edgeSchema.getSource().get().getIdField())
                .collect(Collectors.toSet());

        QueryBuilder idPromiseQueryBuilder = this.idPromiseQueryBuilderFactory.getPromiseQueryBuilder(new IdPromiseEdgeInput(input.getIdPromisesBulk(), edgeSchemas));
        Map<String, QueryBuilder> bulkMap = new HashMap<>();
        bulkMap.put("idPromiseFilter", idPromiseQueryBuilder);

        Map<String, QueryBuilder> predicatesMap = new HashMap<>();
        if (input.getTraversalPromisesPredicates() != null &&
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0) {
            // if we do have traversal promise predicates, we must build filter aggregations for them.
            for (TraversalPromise traversalPromisePredicate : input.getTraversalPromisesPredicates()) {
                QueryBuilder traversalPromiseQueryBuilder = this.traversalPromiseQueryBuilderFactory.getPromiseQueryBuilder(
                        new TraversalPromiseEdgeInput(
                                traversalPromisePredicate,
                                input.getSearchBuilder(),
                                edgeSchemas,
                                TraversalPromiseEdgeInput.EdgeEnd.destination));

                // add the promise query builder as a filter to the predicates promises filter
                predicatesMap.put(traversalPromisePredicate.getId().toString(), traversalPromiseQueryBuilder);
            }
        }

        super.addBulkAndPredicatesPromisesToQuery(bulkMap, predicatesMap, input.getSearchBuilder().getQueryBuilder());

        return StreamSupport.stream(input.getIdPromisesBulk().spliterator(), false).count() > 0 ||
                StreamSupport.stream(input.getTraversalPromisesPredicates().spliterator(), false).count() > 0;
    }
    //endregion

    //region Private Methods
    private QueryBuilder buildPromiseQueryFilter(Iterable<IdPromise> idPromises, Set<String> sourceIdFields) {
        List<Object> ids = StreamSupport.stream(idPromises.spliterator(), false).map(idPromise -> idPromise.getId()).collect(Collectors.toList());

        if (sourceIdFields.size() == 1) {
            return new QueryBuilder().query().filtered().filter(PromiseStringConstants.PROMISE_SCHEMAS_ROOT).terms(sourceIdFields.iterator().next(), ids);
        } else {
            QueryBuilder queryBuilder = new QueryBuilder();
            for(String sourceIdField : sourceIdFields) {
                queryBuilder.seekRoot().query().filtered().filter(PromiseStringConstants.PROMISE_SCHEMAS_ROOT).bool().should().terms(sourceIdField, ids);
            }
            return queryBuilder;
        }

    }
    //endregion

    //region Fields
    private QueryBuilderFactory<IdPromiseEdgeInput> idPromiseQueryBuilderFactory;
    //endregion
}
