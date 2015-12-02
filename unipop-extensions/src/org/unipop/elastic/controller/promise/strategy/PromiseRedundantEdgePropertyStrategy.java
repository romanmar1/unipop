package org.unipop.elastic.controller.promise.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.process.strategy.UniGraphPredicatesStrategy;
import org.unipop.process.strategy.UniGraphStartStepStrategy;
import org.unipop.process.strategy.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Karni on 11/30/2015.
 */
public class PromiseRedundantEdgePropertyStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> implements TraversalStrategy.VendorOptimizationStrategy {
    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends VendorOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.VendorOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        priorStrategies.add(UniGraphPredicatesStrategy.class);
        priorStrategies.add(PromisePredicatesStrategy.class);
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

        handleEdgeVertexStep(traversal);
        handleUniGraphVertexStep(traversal);

    }
    //endregion

    //region Private Methods
    private void handleEdgeVertexStep(Traversal.Admin<?, ?> traversal) {
        List<EdgeVertexStep> edgeVertexSteps = TraversalHelper.getStepsOfAssignableClassRecursively(EdgeVertexStep.class, traversal);
        // find all edgeVertexSteps
        for (EdgeVertexStep edgeVertexStep : edgeVertexSteps) {
            // for each edgeVertexStep make sure that its previous step is a UniGraphVertexStep
            if (UniGraphVertexStep.class.isAssignableFrom(edgeVertexStep.getPreviousStep().getClass())) {
                Step nextStep = edgeVertexStep.getNextStep();
                while (HasStep.class.isAssignableFrom(nextStep.getClass())) {
                    HasStep hasStep = (HasStep)nextStep;
                    List<HasContainer> predicatesPromises = new ArrayList<>();
                    for (HasContainer hasContainer : (List<HasContainer>)hasStep.getHasContainers()) {
                        if (hasContainer.getKey().toLowerCase().equals("promise")) {
                            hasContainer.setKey(PromiseStrings.HasKeys.PREDICATES_PROMISE);
                            predicatesPromises.add(hasContainer);
                            hasStep.getHasContainers().remove(hasContainer);
                        }
                    }
                    // Add all containers with "promise" key to previous UniGraphVertexStep
                    ((UniGraphVertexStep)(edgeVertexStep.getPreviousStep())).getPredicates().hasContainers.addAll(predicatesPromises);
                    nextStep = nextStep.getNextStep();

                    if (hasStep.getHasContainers().isEmpty()) {
                        hasStep.getTraversal().removeStep(hasStep);
                    }
                }
            }
        }
    }

    private void handleUniGraphVertexStep(Traversal.Admin<?, ?> traversal) {
        List<UniGraphVertexStep> vertexSteps = TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphVertexStep.class, traversal);
        // find all vertexSteps
        for (UniGraphVertexStep vertexStep : vertexSteps) {
            // for each vertexStep which its return type is vertex and is UniGraphVertexStep
            if (Vertex.class.isAssignableFrom(vertexStep.getReturnClass())) {
                for (HasContainer hasContainer : vertexStep.getPredicates().hasContainers) {
                    if (hasContainer.getKey().toLowerCase().equals("promise")) {
                        hasContainer.setKey(PromiseStrings.HasKeys.PREDICATES_PROMISE);
                    }
                }

            }
        }
    }
    //endregion
}
