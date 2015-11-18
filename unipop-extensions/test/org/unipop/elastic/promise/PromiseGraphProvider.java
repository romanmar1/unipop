package org.unipop.elastic.promise;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic.controller.schema.SchemaControllerManager;
import org.unipop.elastic.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.controller.promise.PromiseControllerManager;
import org.unipop.process.strategy.BasicStrategyRegistrar;
import org.unipop.structure.UniGraph;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by Roman on 11/17/2015.
 */
@SuppressWarnings("Duplicates")
public class PromiseGraphProvider extends AbstractGraphProvider {
    private static String CLUSTER_NAME = "test";
    private final Client client;

    public PromiseGraphProvider() throws IOException, ExecutionException, InterruptedException {
        //patch for failing IO tests that wrute to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.cluster.address", "127.0.0.1:" + client.settings().get("transport.tcp.port"));
        }};
    }

    @Override
    public Configuration newGraphConfiguration(String graphName, Class<?> test, String testMethodName, Map<String, Object> configurationOverrides, LoadGraphWith.GraphData loadGraphWith) {
        Configuration configuration = super.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        configuration.setProperty("controllerManagerFactory", (ControllerManagerFactory)() -> new PromiseControllerManager());
        configuration.setProperty("strategyRegistrar", new BasicStrategyRegistrar());

        ElasticGraphConfiguration elasticConfiguration = new ElasticGraphConfiguration(configuration);
        elasticConfiguration.setElasticGraphSchemaProviderFactory(() -> new ModernGraphElementSchemaProvider(graphName.toLowerCase()));
        elasticConfiguration.setElasticGraphDefaultSearchSize(10000);
        elasticConfiguration.setElasticGraphScrollSize(1000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsSize(100000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsShardSize(100000);
        elasticConfiguration.setElasticGraphAggregationsDefaultTermsExecutonHint("global_ordinals_hash");
        //elasticConfiguration.setClusterAddress("some-server:9300");
        //elasticConfiguration.setClusterName("some.remote.cluster");

        return elasticConfiguration;
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            String indexName = configuration.getString("graphName");
            ElasticHelper.clearIndex(client, indexName);
            g.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return null;
    }
}
