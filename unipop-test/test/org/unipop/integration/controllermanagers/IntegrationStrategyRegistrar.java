package org.unipop.integration.controllermanagers;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.process.strategy.*;
import org.unipop.structure.UniGraph;

public class IntegrationStrategyRegistrar implements StrategyRegistrar {
    @Override
    public void register() {
        try {
            DefaultTraversalStrategies strategies = new DefaultTraversalStrategies();
            strategies.addStrategies(
                    //add strategies here
                    new UniGraphStartStepStrategy(),
                    new UniGraphVertexStepStrategy(),
                    new UniGraphPredicatesStrategy()
            );

            TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().forEach(strategies::addStrategies);
            TraversalStrategies.GlobalCache.registerStrategies(UniGraph.class, strategies);
        } catch (Exception ex) {
            //TODO: something productive
        }
    }
}

