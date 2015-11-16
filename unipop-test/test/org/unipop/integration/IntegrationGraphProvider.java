package org.unipop.integration;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.unipop.controllerprovider.ControllerManagerFactory;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.integration.controllermanagers.IntegrationControllerManager;
import org.unipop.integration.controllermanagers.IntegrationStrategyRegistrar;
import org.unipop.process.strategy.DefaultStrategyRegistrar;
import org.unipop.structure.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class IntegrationGraphProvider extends AbstractGraphProvider {

    private static String CLUSTER_NAME = "test";

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BaseEdge.class);
        add(BaseElement.class);
        add(UniGraph.class);
        add(BaseProperty.class);
        add(BaseVertex.class);
        add(BaseVertexProperty.class);
    }};

    private final Client client;
    private final Connection jdbcConnection;

    public IntegrationGraphProvider() throws IOException, ExecutionException, InterruptedException, SQLException, ClassNotFoundException {
        //patch for failing IO tests that wrute to disk
        System.setProperty("build.dir", System.getProperty("user.dir") + "\\build");
        //Delete elasticsearch 'data' directory
        String path = new java.io.File( "." ).getCanonicalPath() + "\\data";
        File file = new File(path);
        FileUtils.deleteQuietly(file);

        Node node = ElasticClientFactory.createNode(CLUSTER_NAME, false, 0);
        client = node.client();

        Class.forName("org.h2.Driver");
        this.jdbcConnection = DriverManager.getConnection("jdbc:h2:~/test", "sa", "");
        this.jdbcConnection.createStatement().execute("CREATE TABLE IF NOT EXISTS PERSON(id int NOT NULL PRIMARY KEY, name varchar(100), age int, known_by int, known_weight float);");
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
            put("elasticsearch.client", ElasticClientFactory.ClientType.TRANSPORT_CLIENT);
            put("elasticsearch.cluster.name", CLUSTER_NAME);
            put("elasticsearch.cluster.address", "127.0.0.1:" + client.settings().get("transport.tcp.port"));
            put("controllerManagerFactory", (ControllerManagerFactory) IntegrationControllerManager::new);
            put("startegyRegistrar", new IntegrationStrategyRegistrar());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) {
            String indexName = configuration.getString("graphName");
            ElasticHelper.clearIndex(client, indexName);
            g.close();
        }

        jdbcConnection.createStatement().execute("TRUNCATE TABLE PERSON;");
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }

}
