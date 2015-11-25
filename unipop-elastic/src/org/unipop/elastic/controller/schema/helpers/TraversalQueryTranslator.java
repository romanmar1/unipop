package org.unipop.elastic.controller.schema.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.unipop.elastic.controller.ExistsP;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by Roman on 11/17/2015.
 */
public class TraversalQueryTranslator extends TraversalVisitor{
    //region Constructor
    public TraversalQueryTranslator(SearchBuilder searchBuilder, QueryBuilder queryBuilder) {
        this.searchBuilder = searchBuilder;
        this.queryBuilder = queryBuilder;
        this.sequenceSupplier = () -> this.sequenceNumber++;
    }
    //endregion

    //Override Methods
    @Override
    protected void visitRecursive(Object o) {
        this.queryBuilder.push();
        super.visitRecursive(o);
        this.queryBuilder.pop();
    }

    @Override
    protected void visitNotStep(NotStep notStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "mustNot_" + nextSequenceNumber;
        queryBuilder.bool().mustNot(currentLabel);

        super.visitNotStep(notStep);
    }

    @Override
    protected void visitOrStep(OrStep orStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "should_" + nextSequenceNumber;
        queryBuilder.bool().should(currentLabel);

        super.visitOrStep(orStep);
    }

    @Override
    protected void visitAndStep(AndStep andStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "must_" + nextSequenceNumber;
        queryBuilder.bool().must(currentLabel);

        super.visitAndStep(andStep);
    }

    @Override
    protected void visitHasStep(HasStep hasStep) {
        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();

        if (hasStep.getHasContainers().size() == 1) {
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, queryBuilder, (HasContainer)hasStep.getHasContainers().get(0));
        } else {
            int nextSequenceNumber = sequenceSupplier.get();
            String currentLabel = "must_" + nextSequenceNumber;
            queryBuilder.bool().must(currentLabel);

            hasStep.getHasContainers().forEach(hasContainer -> {
                queryBuilder.seek(currentLabel);
                hasContainersQueryTranslator.applyHasContainer(searchBuilder, queryBuilder, (HasContainer) hasContainer);
            });
        }
    }

    @Override
    protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
        if (traversalFilterStep.getLocalChildren().size() == 1) {
            Traversal.Admin subTraversal = (Traversal.Admin)traversalFilterStep.getLocalChildren().get(0);
            if (subTraversal.getSteps().size() == 1
                    && PropertiesStep.class.isAssignableFrom(subTraversal.getSteps().get(0).getClass())) {
                PropertiesStep propertiesStep = (PropertiesStep) subTraversal.getSteps().get(0);

                if (propertiesStep.getPropertyKeys().length == 1) {
                    this.visitRecursive(new HasStep<>(null, new HasContainer(propertiesStep.getPropertyKeys()[0], new ExistsP<Object>())));
                } else {
                    int nextSequenceNumber = sequenceSupplier.get();
                    String currentLabel = "should_" + nextSequenceNumber;
                    queryBuilder.bool().should(currentLabel);

                    for (String key : propertiesStep.getPropertyKeys()) {
                        queryBuilder.seek(currentLabel);
                        this.visitRecursive(new HasStep<>(null, new HasContainer(key, new ExistsP<Object>())));
                    }
                }
            }
        }
    }
    //endregion

    //region Fields
    private SearchBuilder searchBuilder;
    private QueryBuilder queryBuilder;
    private int sequenceNumber = 0;
    private Supplier<Integer> sequenceSupplier;
    //endregion
}
