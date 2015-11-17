package org.unipop.elastic.controller.schema.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.elastic.controller.ExistsP;

import java.util.List;
import java.util.function.Supplier;

/**
 * Created by Roman on 11/17/2015.
 */
public class TraversalQueryTranslator {
    //Public Methods
    public void applyTraversal(SearchBuilder searchBuilder, QueryBuilder queryBuilder, Traversal traversal) {
        buildQuery(
                traversal,
                searchBuilder,
                queryBuilder,
                null,
                () -> sequenceNumber++);
    }
    //endregion

    //Private Methods
    private void buildQuery(Object o, SearchBuilder searchBuilder, QueryBuilder queryBuilder, String parentLabel, Supplier<Integer> sequenceSupplier) {
        if (Traversal.class.isAssignableFrom(o.getClass())) {
            buildTraversalQuery((Traversal) o, searchBuilder, queryBuilder, parentLabel, sequenceSupplier);
        } else if (o.getClass() == OrStep.class) {
            buildOrStepQuery((OrStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == AndStep.class) {
            buildAndStepQuery((AndStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == NotStep.class) {
            buildNotStepQuery((NotStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == HasStep.class) {
            buildHasStepQuery((HasStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == TraversalFilterStep.class) {
            buildTraversalFilterStepQuery((TraversalFilterStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else {
            //TODO: allow configurable behvaior for unsupported or unepxected elements
            throw new UnsupportedOperationException(o.getClass() + " is not supported in promise conditions");
        }

        // back to previous position
        if (parentLabel == null) {
            queryBuilder.seekRoot();
        } else {
            queryBuilder.seek(parentLabel);
        }
    }

    private void buildNotStepQuery(NotStep notStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "mustNot_" + nextSequenceNumber;

        queryBuilder.bool().mustNot(currentLabel);
        notStep.getLocalChildren().forEach(child -> buildQuery((Traversal) child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildTraversalQuery(Traversal traversal, SearchBuilder searchBuilder, QueryBuilder queryBuilder, String parentLabel, Supplier<Integer> sequenceSupplier) {
        List<Step> steps = traversal.asAdmin().getSteps();
        for (Step step : steps) {
            buildQuery(step, searchBuilder, queryBuilder, parentLabel, sequenceSupplier);
        }
    }

    private void buildOrStepQuery(OrStep orStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "should_" + nextSequenceNumber;

        queryBuilder.bool().should(currentLabel);
        orStep.getLocalChildren().forEach(child -> buildQuery(child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildAndStepQuery(AndStep andStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "must_" + nextSequenceNumber;

        queryBuilder.bool().must(currentLabel);
        andStep.getLocalChildren().forEach(child -> buildQuery((Traversal) child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildHasStepQuery(HasStep hasStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "bool_" + nextSequenceNumber;
        queryBuilder.bool(currentLabel);

        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();
        hasStep.getHasContainers().forEach(hasContainer -> {
            queryBuilder.seek(currentLabel);
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, queryBuilder, (HasContainer) hasContainer);
        });
    }

    private void buildTraversalFilterStepQuery(TraversalFilterStep traversalFilterStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "should_" + nextSequenceNumber;
        queryBuilder.bool().should(currentLabel);

        if (traversalFilterStep.getLocalChildren().size() == 1) {
            Traversal.Admin subTraversal = (Traversal.Admin)traversalFilterStep.getLocalChildren().get(0);
            if (subTraversal.getSteps().size() == 1
                    && PropertiesStep.class.isAssignableFrom(subTraversal.getSteps().get(0).getClass())) {
                PropertiesStep propertiesStep = (PropertiesStep) subTraversal.getSteps().get(0);

                for (String key : propertiesStep.getPropertyKeys()) {
                    queryBuilder.seek(currentLabel);
                    buildQuery(new HasStep<>(null, new HasContainer(key, new ExistsP<Object>())), searchBuilder, queryBuilder, currentLabel, sequenceSupplier);
                }
            }
        }
    }
    //endregion

    //region Fields
    private int sequenceNumber = 0;
    //endregion
}
