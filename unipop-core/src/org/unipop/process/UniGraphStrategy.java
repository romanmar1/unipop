package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.controller.Predicates;
import org.unipop.structure.UniGraph;

public class UniGraphStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> {
    private static final UniGraphStrategy INSTANCE = new UniGraphStrategy();
    public static UniGraphStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) return;

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) return;
        UniGraph uniGraph = (UniGraph) graph;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(graphStep -> {
            if(graphStep.getIds().length > 0) return; //let Graph.vertices(ids) handle it.

            Predicates predicates = getPredicates(graphStep);
            final UniGraphStartStep<?> uniGraphStartStep = new UniGraphStartStep<>(graphStep, predicates, uniGraph.getControllerManager());
            TraversalHelper.replaceStep(graphStep, (Step) uniGraphStartStep, traversal);
        });

        TraversalHelper.getStepsOfClass(VertexStep.class, traversal).forEach(vertexStep -> {
            boolean returnVertex = vertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates = returnVertex ? new Predicates() : getPredicates(vertexStep);

            UniGraphVertexStep uniGraphVertexStep = new UniGraphVertexStep(vertexStep, predicates, uniGraph.getControllerManager());
            TraversalHelper.replaceStep(vertexStep, uniGraphVertexStep, traversal);
        });

    }

    private Predicates getPredicates(Step step){
        Predicates predicates = new Predicates();
        predicates.labels = step.getLabels();

        while(predicates.labels.size() == 0) {
            step = step.getNextStep();
            if(step instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) step;
                hasContainerHolder.getHasContainers().forEach(predicates.hasContainers::add);
                predicates.labels = step.getLabels();
                step.getTraversal().removeStep(step);
            }
            else if(step instanceof RangeGlobalStep) {
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                predicates.labels = step.getLabels();
            }
            else break;
        }
        return predicates;
    }
}
