package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.schema.helpers.TraversalVisitor;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Created by Roman on 11/28/2015.
 */
public class TraversalConcatKeyIdProvider implements TraversalIdProvider<String> {

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
        protected void visitHasStep(HasStep hasStep) {
            Seq.seq(hasStep.getHasContainers()).map(hasContainer -> ((HasContainer)hasContainer).getKey()).forEach(key -> this.builder.append(key));
        }

        @Override
        protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
            if (traversalFilterStep.getLocalChildren().size() == 1) {
                Traversal.Admin subTraversal = (Traversal.Admin) traversalFilterStep.getLocalChildren().get(0);
                if (subTraversal.getSteps().size() == 1
                        && PropertiesStep.class.isAssignableFrom(subTraversal.getSteps().get(0).getClass())) {
                    PropertiesStep propertiesStep = (PropertiesStep) subTraversal.getSteps().get(0);
                    Seq.of(propertiesStep.getPropertyKeys()).forEach(key -> this.builder.append(key));
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
