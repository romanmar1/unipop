package org.unipop.process.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.elastic.controller.ExistsP;
import org.unipop.elastic.controller.Predicates;

import java.util.Arrays;

/**
 * Created by Karni on 11/26/2015.
 */
public class HasContainersToPredicatesTransformer {
    //region Private Methods
    public Predicates transformAndRemove(Step step, Traversal.Admin traversal){
        Predicates predicates = new Predicates();

        while(true) {
            if(step instanceof HasContainerHolder) {
                HasContainerHolder hasContainerHolder = (HasContainerHolder) step;
                hasContainerHolder.getHasContainers().forEach(predicates.hasContainers::add);
                traversal.removeStep(step);

                if(collectLabels(predicates, step)) {
                    return predicates;
                }
            }
            else if (TraversalFilterStep.class.isAssignableFrom(step.getClass())) {
                TraversalFilterStep traversalFilterStep = (TraversalFilterStep)step;
                for(Object localChild : traversalFilterStep.getLocalChildren()) {
                    Traversal.Admin filterTraversal = (Traversal.Admin)localChild;
                    Predicates childPredicates = transformAndRemove(filterTraversal.getStartStep(), filterTraversal);
                    childPredicates.hasContainers.forEach(predicates.hasContainers::add);
                    childPredicates.labels.forEach(predicates.labels::add);

                    if (filterTraversal.getSteps().size() == 0) {
                        traversal.removeStep(traversalFilterStep);
                    }

                    collectLabels(predicates, step);
                    return predicates;
                }
            }
            else if (PropertiesStep.class.isAssignableFrom(step.getClass()) &&
                    step.equals(traversal.getEndStep()) &&
                    TraversalFilterStep.class.isAssignableFrom(traversal.getParent().getClass())) {
                PropertiesStep propertiesStep = (PropertiesStep)step;
                Arrays.asList(propertiesStep.getPropertyKeys()).forEach(propertyKey -> {
                    predicates.hasContainers.add(new HasContainer(propertyKey, new ExistsP()));
                });
                traversal.removeStep(step);

                if(collectLabels(predicates, step)) {
                    return predicates;
                }
            }
            else if(step instanceof RangeGlobalStep) {
                RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                predicates.limitHigh = rangeGlobalStep.getHighRange();
                if(collectLabels(predicates, step)) return predicates;
            }
            else {
                return predicates;
            }

            step = step.getNextStep();
        }
    }

    private boolean collectLabels(Predicates predicates, Step<?, ?> step) {
        step.getLabels().forEach(predicates.labels::add);
        return step.getLabels().size() > 0;
    }
    //endregion
}
