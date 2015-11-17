package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.UniGraph;

/**
 * Created by Roman on 11/17/2015.
 */
@SuppressWarnings("Duplicates")
public class BasicStrategyRegistrar implements StrategyRegistrar {
    //region org.unipop.process.strategy.StrategyRegistrar Implementation
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
    //endregion
}
