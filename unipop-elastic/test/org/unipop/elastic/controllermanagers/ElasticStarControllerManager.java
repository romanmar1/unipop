package org.unipop.elastic.controllermanagers;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.Client;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic.controller.star.ElasticStarController;
import org.unipop.elastic.controller.star.inneredge.nested.NestedEdgeController;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.util.Map;

public class ElasticStarControllerManager extends BasicControllerManager {
    private ElasticStarController controller;
    private Client client;
    private ElasticMutations elasticMutations;
    private TimingAccessor timing;
    private String indexName;

    @Override
    protected VertexController getDefaultVertexController() {
        return controller;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return controller;
    }

    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        this.indexName = configuration.getString("graphName", "unipop");

        client = ElasticClientFactory.create(configuration);
        ElasticHelper.createIndex(indexName, client);

        timing = new TimingAccessor();
        elasticMutations = new ElasticMutations(false, client, timing);
        controller = new ElasticStarController(graph, client, elasticMutations ,indexName, 0, timing);
    }

    @Override
    public void commit() {
        elasticMutations.commit();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        ElasticHelper.mapNested(client, indexName, outV.label(), label);
        controller.addEdgeMapping(new NestedEdgeController(outV.label(), label, Direction.OUT, "vertex_id", inV.label(), "edge_id"));
        return super.addEdge(edgeId, label, outV, inV, properties);
    }

    @Override
    public void close() {
        client.close();
        timing.print();
    }
}
