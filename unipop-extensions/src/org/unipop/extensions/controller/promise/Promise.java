package org.unipop.extensions.controller.promise;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.controller.ExistsP;
import org.unipop.elastic.controller.schema.helpers.HasContainersTranslator;
import org.unipop.elastic.controller.schema.helpers.QueryBuilder;
import org.unipop.elastic.controller.schema.helpers.SearchBuilder;

import java.util.List;
import java.util.function.Supplier;

/**
 * Created by Roman on 11/16/2015.
 */
public class Promise {

    //region Ctor
    public Promise(Traversal traversal) {
        promiseTraversal = traversal;
    }
    //endregion

    //Public Methods
    public void addToQuery(SearchBuilder searchBuilder, QueryBuilder queryBuilder) {
        searchBuilder.getQueryBuilder().query().filtered().filter();

        buildPromiseQuery(
                promiseTraversal,
                searchBuilder,
                queryBuilder,
                null,
                new Supplier<Integer>() {
                    @Override
                    public Integer get() {
                        return sequenceNumber++;
                    }

                    private int sequenceNumber = 0;
                });
    }
    //endregion

    //Private Methods
    private void buildPromiseQuery(Object o, SearchBuilder searchBuilder, QueryBuilder queryBuilder, String parentLabel, Supplier<Integer> sequenceSupplier) {
          if (Traversal.class.isAssignableFrom(o.getClass())) {
            buildTraversalPromiseQuery((Traversal) o, searchBuilder, queryBuilder, parentLabel, sequenceSupplier);
        } else if (o.getClass() == OrStep.class) {
            buildOrStepPromiseQuery((OrStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == AndStep.class) {
            buildAndStepPromiseQuery((AndStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == NotStep.class) {
              buildNotStepPromiseQuery((NotStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == HasStep.class) {
            buildHasStepPromiseQuery((HasStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else if (o.getClass() == TraversalFilterStep.class) {
              buildTraversalFilterStepPromiseQuery((TraversalFilterStep) o, searchBuilder, queryBuilder, sequenceSupplier);
        } else {
            throw new UnsupportedOperationException(o.getClass() + " is not supported in promise conditions");
        }

        // back to previous position
        if (parentLabel == null) {
            queryBuilder.seekRoot();
        } else {
            queryBuilder.seek(parentLabel);
        }
    }

    private void buildNotStepPromiseQuery(NotStep notStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "mustNot_" + nextSequenceNumber;

        queryBuilder.bool().mustNot(currentLabel);
        notStep.getLocalChildren().forEach(child -> buildPromiseQuery((Traversal) child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildTraversalPromiseQuery(Traversal traversal, SearchBuilder searchBuilder, QueryBuilder queryBuilder, String parentLabel, Supplier<Integer> sequenceSupplier) {
        List<Step> steps = traversal.asAdmin().getSteps();
        for (Step step : steps) {
            buildPromiseQuery(step, searchBuilder, queryBuilder, parentLabel, sequenceSupplier);
        }
    }

    private void buildOrStepPromiseQuery(OrStep orStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "should_" + nextSequenceNumber;

        queryBuilder.bool().should(currentLabel);
        orStep.getLocalChildren().forEach(child -> buildPromiseQuery(child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildAndStepPromiseQuery(AndStep andStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "must_" + nextSequenceNumber;

        queryBuilder.bool().must(currentLabel);
        andStep.getLocalChildren().forEach(child -> buildPromiseQuery((Traversal) child, searchBuilder, queryBuilder, currentLabel, sequenceSupplier));
    }

    private void buildHasStepPromiseQuery(HasStep hasStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "bool_" + nextSequenceNumber;
        queryBuilder.bool(currentLabel);

        HasContainersTranslator hasContainersTranslator = new HasContainersTranslator();
        hasStep.getHasContainers().forEach(hasContainer -> {
            queryBuilder.seek(currentLabel);
            hasContainersTranslator.applyHasContainer(searchBuilder, queryBuilder, (HasContainer) hasContainer);
        });
    }

    private void buildTraversalFilterStepPromiseQuery(TraversalFilterStep traversalFilterStep, SearchBuilder searchBuilder, QueryBuilder queryBuilder, Supplier<Integer> sequenceSupplier) {
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
                    buildPromiseQuery(new HasStep<>(null, new HasContainer(key, new ExistsP<Object>())), searchBuilder, queryBuilder, currentLabel, sequenceSupplier);
                }
            }
        }
    }
    //endregion

    //region Fields
    private Traversal promiseTraversal;
    //endregion
}
