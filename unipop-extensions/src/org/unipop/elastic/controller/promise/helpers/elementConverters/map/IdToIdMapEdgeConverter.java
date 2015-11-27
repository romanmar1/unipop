package org.unipop.elastic.controller.promise.helpers.elementConverters.map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.IdPromise;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.promise.helpers.PromiseStringConstants;
import org.unipop.elastic.controller.schema.helpers.MapHelper;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Roman on 11/27/2015.
 */
public class IdToIdMapEdgeConverter extends GraphPromiseMapEdgeConverterBase {
    //region Constructor
    public IdToIdMapEdgeConverter(UniGraph graph, Direction direction) {
        super(graph, direction);
    }
    //endregion

    //region GraphPromiseMapEdgeConverterBase Implementation
    @Override
    public boolean canConvert(Map<String, Object> map) {
        Map<String, Object> bulkIdPromisesMap = MapHelper.value(map, bulkIdPromisesKey);
        if (bulkIdPromisesMap == null || bulkIdPromisesMap.size() == 0) {
            return false;
        }

        Map<String, Object> reducedIdPromisesMap = MapHelper.value(bulkIdPromisesMap,
                bulkIdPromisesMap.keySet().iterator().next() + "." + PromiseStringConstants.REDUCED_ID_PROMISES);
        if (reducedIdPromisesMap == null || reducedIdPromisesMap.size() == 0) {
            return false;
        }

        return true;
    }

    @Override
    public Iterable<Element> convert(Map<String, Object> map) {
        List<Element> edges = new ArrayList<>();

        Map<String, Object> bulkIdPromisesMap = MapHelper.value(map, bulkIdPromisesKey);
        for(Map.Entry<String, Object> layerOneEntry : bulkIdPromisesMap.entrySet()) {
            Map<String, Object> reducedIdPromiseMap = MapHelper.value(bulkIdPromisesMap, layerOneEntry.getKey() + "." + PromiseStringConstants.REDUCED_ID_PROMISES);
            if (reducedIdPromiseMap == null) {
                continue;
            }

            for(Map.Entry<String, Object> layerTwoEntry : reducedIdPromiseMap.entrySet()) {

                // might not be neccessary if the query appenders added having clauses to filter out unwanted buckets
                // if they hadn't, we'll need to add the edge properties (like count) to the actual edge promise and let gremlin
                // filter them out using stock HasStep, which will require either modifying a strategy or implementing this via Edge Controller somehow....
                Map<String, Object> edgeProperties = (Map<String, Object>)layerTwoEntry.getValue();

                PromiseVertex outVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                        new PromiseVertex(new IdPromise(layerOneEntry.getKey()), this.graph) :
                        new PromiseVertex(new IdPromise(layerTwoEntry.getKey()), this.graph);

                PromiseVertex inVertex = direction == Direction.OUT || direction == Direction.BOTH ?
                        new PromiseVertex(new IdPromise(layerTwoEntry.getKey()), this.graph) :
                        new PromiseVertex(new IdPromise(layerOneEntry.getKey()), this.graph);

                edges.add(new PromiseEdge(super.getEdgeId(outVertex, inVertex), outVertex, inVertex, this.graph));
            }
        }

        return edges;
    }
    //endregion

    //region Fields
    private String bulkIdPromisesKey =
            PromiseStringConstants.BULK_ID_PROMISES_FILTERS + "." +
            PromiseStringConstants.BULK_ID_PROMISES_FILTER + "." +
            PromiseStringConstants.BULK_ID_PROMISES;
    //endregion
}
