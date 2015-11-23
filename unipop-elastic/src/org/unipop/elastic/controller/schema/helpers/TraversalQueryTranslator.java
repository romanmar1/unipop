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
        super.visitRecursive(o);

        // back to previous position
        if (this.lastParentLabel == null) {
            queryBuilder.seekRoot();
        } else {
            queryBuilder.seek(this.lastParentLabel);
        }
    }

    @Override
    protected void visitNotStep(NotStep notStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "mustNot_" + nextSequenceNumber;
        queryBuilder.bool().mustNot(currentLabel);

        String currentParentLabel = this.lastParentLabel;
        this.lastParentLabel = currentLabel;
        super.visitNotStep(notStep);
        this.lastParentLabel = currentParentLabel;
    }

    @Override
    protected void visitOrStep(OrStep orStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "should_" + nextSequenceNumber;
        queryBuilder.bool().should(currentLabel);

        String currentParentLabel = this.lastParentLabel;
        this.lastParentLabel = currentLabel;
        super.visitOrStep(orStep);
        this.lastParentLabel = currentParentLabel;
    }

    @Override
    protected void visitAndStep(AndStep andStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "must_" + nextSequenceNumber;
        queryBuilder.bool().must(currentLabel);

        String currentParentLabel = this.lastParentLabel;
        this.lastParentLabel = currentLabel;
        super.visitAndStep(andStep);
        this.lastParentLabel = currentParentLabel;
    }

    @Override
    protected void visitHasStep(HasStep hasStep) {
        int nextSequenceNumber = sequenceSupplier.get();
        String currentLabel = "bool_" + nextSequenceNumber;
        queryBuilder.bool(currentLabel);

        HasContainersQueryTranslator hasContainersQueryTranslator = new HasContainersQueryTranslator();
        hasStep.getHasContainers().forEach(hasContainer -> {
            queryBuilder.seek(currentLabel);
            hasContainersQueryTranslator.applyHasContainer(searchBuilder, queryBuilder, (HasContainer) hasContainer);
        });
    }

    @Override
    protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
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
                    this.visitRecursive(new HasStep<>(null, new HasContainer(key, new ExistsP<Object>())));
                }
            }
        }
    }
    //endregion

    //region Fields
    private SearchBuilder searchBuilder;
    private QueryBuilder queryBuilder;
    private String lastParentLabel;
    private int sequenceNumber = 0;
    private Supplier<Integer> sequenceSupplier;
    //endregion
}
