package org.unipop.extensions.controller.promise;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.elastic.controller.schema.SchemaEdgeController;
import org.unipop.elastic.controller.schema.SchemaVertexController;
import org.unipop.elastic.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic.controller.schema.helpers.LazyGetterFactory;
import org.unipop.elastic.controller.schema.helpers.ReflectionHelper;
import org.unipop.elastic.controller.schema.helpers.elementConverters.CompositeElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.DualEdgeConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.SingularEdgeConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.VertexConverter;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.CachedGraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.DefaultGraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProviderFactory;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.Arrays;

/**
 * Created by Roman on 11/16/2015.
 */
public class PromiseControllerManager extends BasicControllerManager {
    //region BasicControllerManager Implementation
    @Override
    protected VertexController getDefaultVertexController() {
        return this.vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return this.edgeController;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void init(UniGraph graph, Configuration configuration) throws Exception {
        ElasticGraphConfiguration elasticConfiguration = new ElasticGraphConfiguration(configuration);

        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());

        if (elasticConfiguration.getElasticGraphSchemaProviderFactory() != null) {
            this.schemaProvider = elasticConfiguration.getElasticGraphSchemaProviderFactory().getSchemaProvider();
        } else {
            try {
                this.schemaProvider = ((GraphElementSchemaProviderFactory) ReflectionHelper.createNew(elasticConfiguration.getElasticGraphSchemaProviderFactoryClass())).getSchemaProvider();
            } catch (Exception ex) {
                this.schemaProvider = new CachedGraphElementSchemaProvider(
                        new DefaultGraphElementSchemaProvider(Arrays.asList("_all"))
                );
            }
        }

        client = ElasticClientFactory.create(elasticConfiguration);

        this.vertexController = new PromiseVertexController();

        this.edgeController = new PromiseEdgeController();
    }

    @Override
    public void commit() {

    }

    @Override
    public void close() {
        if (!isClosed) {
            client.close();
            isClosed = true;
        }
    }
    //endregion

    //region Fields
    private VertexController vertexController;
    private EdgeController edgeController;

    private GraphElementSchemaProvider schemaProvider;

    private Client client;
    private boolean isClosed;
    //endregion
}
