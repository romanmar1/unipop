package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.Predicates;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Roman on 11/8/2015.
 */
public class UniGraphPredicatesStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> implements TraversalStrategy.VendorOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends VendorOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.VendorOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) {
            return;
        }

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) {
            return;
        }

        PredicatesCollector collector = new PredicatesCollector();

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphStartStep.class, traversal).forEach(elasticGraphStep -> {
            if(elasticGraphStep.getIds().length == 0) {
                Predicates predicates = collector.getPredicates(elasticGraphStep.getNextStep(), traversal);
                elasticGraphStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
                elasticGraphStep.getPredicates().labels.addAll(predicates.labels);
                elasticGraphStep.getPredicates().labels.forEach(label -> elasticGraphStep.addLabel(label));
                elasticGraphStep.getPredicates().limitHigh = predicates.limitHigh;
            }
        });

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphVertexStep.class, traversal).forEach(elasticVertexStep -> {
            boolean returnVertex = elasticVertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : collector.getPredicates(elasticVertexStep.getNextStep(), traversal);
            elasticVertexStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
            elasticVertexStep.getPredicates().labels.addAll(predicates.labels);
            elasticVertexStep.getPredicates().labels.forEach(label -> elasticVertexStep.addLabel(label));
            elasticVertexStep.getPredicates().limitHigh = predicates.limitHigh;
        });
    }
    //endregion


}
