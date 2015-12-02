package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.IdPromise;
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
public class TraversalToIdMapEdgeConverter extends GraphPromiseMapEdgeConverterBase {
    //region Constructor
    public TraversalToIdMapEdgeConverter(
            UniGraph graph,
            Direction direction,
            Iterable<HasContainer> edgeAggPromiseHasContainers,
            GraphElementSchemaProvider schemaProvider,
            Iterable<TraversalPromise> bulkTraversalPromises) {
        super(graph, direction, edgeAggPromiseHasContainers, schemaProvider);

        this.bulkTraversalPromises = Seq.seq(bulkTraversalPromises).groupBy(traversalPromise -> traversalPromise.getId());
    }
    //endregion

    //region GraphPromiseMapEdgeConverterBase Implementation
    @Override
    public boolean canConvert(Map<String, Object> map) {
        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        if (bulkTraversalPromisesMap == null || bulkTraversalPromisesMap.size() == 0) {
            return false;
        }

        Map<String, Object> reducedIdPromisesMap = MapHelper.value(bulkTraversalPromisesMap,
                bulkTraversalPromisesMap.keySet().iterator().next() + "." + PromiseStrings.REDUCED_ID_PROMISES);
        if (reducedIdPromisesMap == null || reducedIdPromisesMap.size() == 0) {
            return false;
        }

        return true;
    }

    @Override
    public Iterable<Element> convert(Map<String, Object> map) {
        List<Element> edges = new ArrayList<>();

        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        for(Map.Entry<String, Object> layerOneEntry : bulkTraversalPromisesMap.entrySet()) {
            Map<String, Object> reducedIdPromiseMap = MapHelper.value(bulkTraversalPromisesMap, layerOneEntry.getKey() + "." + PromiseStrings.REDUCED_ID_PROMISES);
            if (reducedIdPromiseMap == null) {
                continue;
            }

            for(Map.Entry<String, Object> layerTwoEntry : reducedIdPromiseMap.entrySet()) {
                Map<String, Object> edgeProperties = (Map<String, Object>)layerTwoEntry.getValue();

                PromiseVertex outVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                        new PromiseVertex(this.getBulkTraversalPromiseById(layerOneEntry.getKey()), this.graph) :
                        new PromiseVertex(new IdPromise(layerTwoEntry.getKey()), this.graph);

                PromiseVertex inVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                        new PromiseVertex(new IdPromise(layerTwoEntry.getKey()), this.graph) :
                        new PromiseVertex(this.getBulkTraversalPromiseById(layerOneEntry.getKey()), this.graph);

                edges.add(new PromiseEdge(super.getEdgeId(outVertex, inVertex), outVertex, inVertex, edgeProperties, this.graph));
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
    //endregion

    //region Fields
    private Map<Object, List<TraversalPromise>> bulkTraversalPromises;
    //endregion
}
