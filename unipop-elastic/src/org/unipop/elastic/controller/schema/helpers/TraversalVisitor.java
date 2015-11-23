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
 * Created by Karni on 11/23/2015.
 */
public class TraversalVisitor {
    //Public Methods
    public void visit(Traversal traversal) {
        visitRecursive(traversal);
    }
    //endregion

    //Protected Methods
    protected void visitRecursive(Object o) {
        if (Traversal.class.isAssignableFrom(o.getClass())) {
            visitTraversal((Traversal) o);
        } else if (o.getClass() == OrStep.class) {
            visitOrStep((OrStep) o);
        } else if (o.getClass() == AndStep.class) {
            visitAndStep((AndStep) o);
        } else if (o.getClass() == NotStep.class) {
            visitNotStep((NotStep) o);
        } else if (o.getClass() == HasStep.class) {
            visitHasStep((HasStep) o);
        } else if (o.getClass() == TraversalFilterStep.class) {
            visitTraversalFilterStep((TraversalFilterStep) o);
        } else {
            //TODO: allow configurable behvaior for unsupported or unepxected elements
            throw new UnsupportedOperationException(o.getClass() + " is not supported in promise conditions");
        }
    }

    protected void visitNotStep(NotStep notStep) {
        notStep.getLocalChildren().forEach(child -> visitRecursive((Traversal) child));
    }

    protected void visitTraversal(Traversal traversal) {
        List<Step> steps = traversal.asAdmin().getSteps();
        for (Step step : steps) {
            visitRecursive(step);
        }
    }

    protected void visitOrStep(OrStep orStep) {
        orStep.getLocalChildren().forEach(child -> visitRecursive(child));
    }

    protected void visitAndStep(AndStep andStep) {
        andStep.getLocalChildren().forEach(child -> visitRecursive((Traversal) child));
    }

    protected void visitHasStep(HasStep hasStep) {
    }

    protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
    }
    //endregion
}
