package org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.unipop.elastic.controller.ExistsP;
import org.unipop.elastic.controller.schema.helpers.TraversalVisitor;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Created by Karni on 11/23/2015.
 */
public class TraversalEdgeRedundancyTranslator extends TraversalVisitor{
    //region Constructor
    public TraversalEdgeRedundancyTranslator(GraphEdgeSchema.End edgeEnd) {
        this.edgeEnd = edgeEnd;
    }
    //endregion

    //region Override Methods
    @Override
    protected void visitHasStep(HasStep hasStep) {
        ((List<HasContainer>)hasStep.getHasContainers()).stream().forEach(hasContainer -> {
            Optional<String> redundantPropertyName =  this.edgeEnd.getEdgeRedundancy()
                    .getRedundantPropertyName(hasContainer.getKey());

            if (redundantPropertyName.isPresent()) {
                hasContainer.setKey(redundantPropertyName.get());
            } else {
                // currently undefined if can't translate redundant prop name to edge.
                // TODO: think about it.
            }
        });
    }

    @SuppressWarnings("Duplicates")
    @Override
    protected void visitTraversalFilterStep(TraversalFilterStep traversalFilterStep) {
        if (traversalFilterStep.getLocalChildren().size() == 1) {
            Traversal.Admin subTraversal = (Traversal.Admin)traversalFilterStep.getLocalChildren().get(0);
            if (subTraversal.getSteps().size() == 1
                    && PropertiesStep.class.isAssignableFrom(subTraversal.getSteps().get(0).getClass())) {
                PropertiesStep propertiesStep = (PropertiesStep) subTraversal.getSteps().get(0);

                PropertiesStep translatedPropertiesStep = new PropertiesStep(
                        propertiesStep.getTraversal(),
                        propertiesStep.getReturnType(),
                        Arrays.stream(propertiesStep.getPropertyKeys()).map(property -> {
                            Optional<String> redundantPropertyName =  this.edgeEnd.getEdgeRedundancy()
                                    .getRedundantPropertyName(property);

                            if (redundantPropertyName.isPresent()) {
                                return redundantPropertyName.get();
                            } else {
                                // currently undefined if can't translate redundant prop name to edge.
                                // TODO: think about it.
                                return null;
                            }
                        }).toArray(String[]::new));

                TraversalHelper.replaceStep(propertiesStep, translatedPropertiesStep, propertiesStep.getTraversal());
            }
        }
    }


    //endregion

    //region Fields
    private GraphEdgeSchema.End edgeEnd;
    //endregion
}
