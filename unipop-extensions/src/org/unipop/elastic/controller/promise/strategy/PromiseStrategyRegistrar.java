package org.unipop.elastic.controller.promise.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.process.strategy.*;
import org.unipop.structure.UniGraph;

/**
 * Created by Karni on 11/27/2015.
 */
public class PromiseStrategyRegistrar implements StrategyRegistrar {
    //region Constructor
    public PromiseStrategyRegistrar(GraphElementSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }
    //endregion

    //region org.unipop.process.strategy.StrategyRegistrar Implementation
    @Override
    public void register() {
        try {
            DefaultTraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(
                    //add strategies here
                    new UniGraphStartStepStrategy(),
                    new UniGraphVertexStepStrategy(),
                    new UniGraphPredicatesStrategy(),
                    new PromiseRedundantEdgePropertyStrategy(),
                    new PromisePredicatesStrategy(schemaProvider)
            );

            TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().forEach(strategies::addStrategies);
            TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, strategies);
        } catch (Exception ex) {
            //TODO: something productive
        }
    }
    //endregion

    //region Private Fields
    private final GraphElementSchemaProvider schemaProvider;
    //endregion
}
