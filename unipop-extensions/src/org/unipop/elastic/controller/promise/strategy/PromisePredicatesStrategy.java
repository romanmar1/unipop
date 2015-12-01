package org.unipop.elastic.controller.promise.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeRedundancy;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.process.strategy.HasContainersToPredicatesTransformer;
import org.unipop.process.strategy.UniGraphPredicatesStrategy;
import org.unipop.process.strategy.UniGraphStartStepStrategy;
import org.unipop.process.strategy.UniGraphVertexStepStrategy;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by Karni on 11/26/2015.
 */
@SuppressWarnings("Duplicates")
public class PromisePredicatesStrategy extends AbstractTraversalStrategy<TraversalStrategy.VendorOptimizationStrategy> implements TraversalStrategy.VendorOptimizationStrategy {
    //region Constructor
    public PromisePredicatesStrategy(GraphElementSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }
    //endregion

    //region AbstractTraversalStrategy Implementation
    @Override
    public Set<Class<? extends VendorOptimizationStrategy>> applyPrior() {
        Set<Class<? extends TraversalStrategy.VendorOptimizationStrategy>> priorStrategies = new HashSet<>();
        priorStrategies.add(UniGraphStartStepStrategy.class);
        priorStrategies.add(UniGraphVertexStepStrategy.class);
        priorStrategies.add(UniGraphPredicatesStrategy.class);
        return priorStrategies;
    }

    public void apply(Traversal.Admin<?, ?> traversal) {
        Optional<GraphEdgeSchema> edgeSchema = this.schemaProvider.getEdgeSchema("promise", Optional.of("promise"), Optional.of("promise"));
        if (!edgeSchema.isPresent() || !GraphPromiseEdgeSchema.class.isAssignableFrom(edgeSchema.get().getClass())) {
            return;
        }

        List<String> promisePropertyNames = Seq.seq(((GraphPromiseEdgeSchema)edgeSchema.get()).getProperties())
                .map(property -> property.getName())
                .toList();

        if (promisePropertyNames == null || promisePropertyNames.isEmpty()) {
            return;
        }

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphVertexStep.class, traversal).forEach(uniGraphVertexStep -> {
            if (Edge.class.isAssignableFrom(uniGraphVertexStep.getReturnClass())) {
                List<HasContainer> promisePropertyHasContainers = Seq.seq(uniGraphVertexStep.getPredicates().hasContainers)
                        .filter(hasContainer -> promisePropertyNames.contains(hasContainer.getKey()))
                        .toList();

                promisePropertyHasContainers.forEach(hasContainer -> uniGraphVertexStep.getPredicates().hasContainers.remove(hasContainer));

                HasStep promisePropertyHasStep = new HasStep(traversal, Seq.seq(promisePropertyHasContainers).toArray(HasContainer[]::new));
                TraversalHelper.insertAfterStep(promisePropertyHasStep, uniGraphVertexStep, traversal);
            }
        });
    }

    //region private fields
    private final GraphElementSchemaProvider schemaProvider;
    //endregion
}
