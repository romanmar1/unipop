package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.schema.helpers.TraversalVisitor;

/**
 * Created by Roman on 12/1/2015.
 */
public class TraversalExpressionIdProvider implements TraversalIdProvider<String> {
    //region TraversalIdProvider
    @Override
    public String getId(Traversal traversal) {
        Visitor visitor = new Visitor();
        visitor.visit(traversal);
        return visitor.getBuilder().toString();
    }
    //endregion

    private class Visitor extends TraversalVisitor {
        //region Constructor
        public Visitor() {
            this.builder = new StringBuilder();
        }
        //endregion

        //region Override Methods
        @Override
        protected void visitNotStep(NotStep notStep) {
            this.builder.append("not").append("(");
            notStep.getLocalChildren().forEach(child -> {
                visitRecursive((Traversal) child);
                this.builder.append(", ");
            });
            this.builder.delete(this.builder.length() - 2, this.builder.length()).append(")");
        }

        protected void visitOrStep(OrStep orStep) {
            this.builder.append("or").append("(");
            orStep.getLocalChildren().forEach(child -> {
                visitRecursive(child);
                this.builder.append(", ");
            });
            this.builder.delete(this.builder.length() - 2, this.builder.length()).append(")");
        }

        protected void visitAndStep(AndStep andStep) {
            this.builder.append("and").append("(");
            andStep.getLocalChildren().forEach(child -> {
                visitRecursive((Traversal) child);
                this.builder.append(", ");
            });
            this.builder.delete(this.builder.length() - 2, this.builder.length()).append(")");
        }

        @Override
        protected void visitHasStep(HasStep hasStep) {
            if (hasStep.getHasContainers().size() == 1) {
                Seq.seq(hasStep.getHasContainers()).forEach(hasContainer -> this.builder.append(((HasContainer)hasContainer).toString()));
            } else {
                this.builder.append("and").append("(");
                Seq.seq(hasStep.getHasContainers()).forEach(hasContainer -> this.builder.append(((HasContainer)hasContainer).toString()).append(", "));
                this.builder.delete(this.builder.length() - 2, this.builder.length()).append(")");
            }
        }

        @Override
        protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
            if (traversalFilterStep.getLocalChildren().size() == 1) {
                Traversal.Admin subTraversal = (Traversal.Admin) traversalFilterStep.getLocalChildren().get(0);
                if (subTraversal.getSteps().size() == 1
                        && PropertiesStep.class.isAssignableFrom(subTraversal.getSteps().get(0).getClass())) {
                    PropertiesStep propertiesStep = (PropertiesStep) subTraversal.getSteps().get(0);
                    this.builder.append("exists").append("(");
                    Seq.of(propertiesStep.getPropertyKeys()).forEach(key -> this.builder.append(key).append(", "));
                    this.builder.delete(this.builder.length() - 2, this.builder.length()).append(")");
                }
            }
        }
        //endregion

        //region Properties
        public StringBuilder getBuilder() {
            return this.builder;
        }
        //endregion

        //region Fields
        private StringBuilder builder;
        //endregion
    }
}
