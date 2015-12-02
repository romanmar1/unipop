package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.schema.helpers.MapHelper;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Roman on 11/28/2015.
 */
public class TraversalToTraversalMapEdgeConverter extends GraphPromiseMapEdgeConverterBase {
    //region Constructor
    public TraversalToTraversalMapEdgeConverter(
            UniGraph graph,
            Direction direction,
            Iterable<HasContainer> edgeAggPromiseHasContainers,
            GraphElementSchemaProvider schemaProvider,
            Iterable<TraversalPromise> bulkTraversalPromises,
            Iterable<TraversalPromise> predicatesTraversalPromises) {
        super(graph, direction, edgeAggPromiseHasContainers, schemaProvider);

        this.bulkTraversalPromises = Seq.seq(bulkTraversalPromises).groupBy(traversalPromise -> traversalPromise.getId());
        this.predicatesTraversalPromises = Seq.seq(predicatesTraversalPromises).groupBy(traversalPromise -> traversalPromise.getId());
    }
    //endregion

    //region GraphPromiseMapEdgeConverterBase Implementation
    @Override
    public boolean canConvert(Map<String, Object> map) {
        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        if (bulkTraversalPromisesMap == null || bulkTraversalPromisesMap.size() == 0) {
            return false;
        }

        Map<String, Object> predicatesPromisesMap = MapHelper.value(bulkTraversalPromisesMap,
                bulkTraversalPromisesMap.keySet().iterator().next() + "." + PromiseStrings.PREDICATES_PROMISES);
        if (predicatesPromisesMap == null || predicatesPromisesMap.size() == 0) {
            return false;
        }

        return true;
    }

    @Override
    public Iterable<Element> convert(Map<String, Object> map) {
        List<Element> edges = new ArrayList<>();

        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        for(Map.Entry<String, Object> layerOneEntry : bulkTraversalPromisesMap.entrySet()) {
            Map<String, Object> predicatesPromiseMap = MapHelper.value(bulkTraversalPromisesMap, layerOneEntry.getKey() + "." + PromiseStrings.PREDICATES_PROMISES);
            if (predicatesPromiseMap == null) {
                continue;
            }

            for(Map.Entry<String, Object> layerTwoEntry : predicatesPromiseMap.entrySet()) {
                Map<String, Object> edgeProperties = (Map<String, Object>)layerTwoEntry.getValue();
                long count = (long)edgeProperties.get("count");

                if (count > 0) {
                    PromiseVertex outVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                            new PromiseVertex(this.getBulkTraversalPromiseById(layerOneEntry.getKey()), this.graph) :
                            new PromiseVertex(this.getPredicateTraversalPromiseById(layerTwoEntry.getKey()), this.graph);

                    PromiseVertex inVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                            new PromiseVertex(this.getPredicateTraversalPromiseById(layerTwoEntry.getKey()), this.graph) :
                            new PromiseVertex(this.getBulkTraversalPromiseById(layerOneEntry.getKey()), this.graph);

                    edges.add(new PromiseEdge(super.getEdgeId(outVertex, inVertex), outVertex, inVertex, edgeProperties, this.graph));
                }
            }
        }

        return edges;
    }
    //endregion

    //region Private Methods
    private TraversalPromise getBulkTraversalPromiseById(Object id) {
        List<TraversalPromise> traversalPromises = this.bulkTraversalPromises.get(id);
        if (traversalPromises == null || traversalPromises.size() == 0) {
            return null;
        }

        return traversalPromises.get(0);
    }

    private TraversalPromise getPredicateTraversalPromiseById(Object id) {
        List<TraversalPromise> traversalPromises = this.predicatesTraversalPromises.get(id);
        if (traversalPromises == null || traversalPromises.size() == 0) {
            return null;
        }

        return traversalPromises.get(0);
    }
    //endregion

    //region Fields
    private Map<Object, List<TraversalPromise>> bulkTraversalPromises;
    private Map<Object, List<TraversalPromise>> predicatesTraversalPromises;
    //endregion
}