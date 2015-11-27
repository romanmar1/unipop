package org.unipop.elastic.controller.promise.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.elastic.controller.Predicates;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeRedundancy;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.process.UniGraphStartStep;
import org.unipop.process.UniGraphVertexStep;
import org.unipop.process.strategy.PredicatesCollector;
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
        return priorStrategies;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if(traversal.getEngine().isComputer()) {
            return;
        }

        Graph graph = traversal.getGraph().get();
        if(!(graph instanceof UniGraph)) {
            return;
        }

        PredicatesCollector collector = new PredicatesCollector();

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphStartStep.class, traversal).forEach(elasticGraphStep -> {
            if(elasticGraphStep.getIds().length == 0) {
                Predicates predicates = collector.getPredicates(elasticGraphStep.getNextStep(), traversal);
                elasticGraphStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
                elasticGraphStep.getPredicates().labels.addAll(predicates.labels);
                elasticGraphStep.getPredicates().labels.forEach(label -> elasticGraphStep.addLabel(label));
                elasticGraphStep.getPredicates().limitHigh = predicates.limitHigh;
            }
        });

        TraversalHelper.getStepsOfAssignableClassRecursively(UniGraphVertexStep.class, traversal).forEach(elasticVertexStep -> {
            boolean returnVertex = elasticVertexStep.getReturnClass().equals(Vertex.class);
            Predicates predicates;
            if (returnVertex) {
                predicates = collector.getPredicates(elasticVertexStep.getNextStep(), traversal);
                predicates.hasContainers = translateToEdgeRedundantProperties(elasticVertexStep.getEdgeLabels(), predicates.hasContainers);
            } else {
                predicates = new Predicates();
            }
            
            elasticVertexStep.getPredicates().hasContainers.addAll(predicates.hasContainers);
            elasticVertexStep.getPredicates().labels.addAll(predicates.labels);
            elasticVertexStep.getPredicates().labels.forEach(label -> elasticVertexStep.addLabel(label));
            elasticVertexStep.getPredicates().limitHigh = predicates.limitHigh;
        });
    }
    //endregion

    private ArrayList<HasContainer> translateToEdgeRedundantProperties(String[] labels, List<HasContainer> hasContainers) {
        ArrayList<HasContainer> newHasContainers = new ArrayList<>();
        // for each label
        for (String label : labels) {
            if (schemaProvider.getEdgeSchemas(label).isPresent()) {
                // for each schema of the label
                for (GraphEdgeSchema edgeSchema : schemaProvider.getEdgeSchemas(label).get()) {
                    if (edgeSchema.getDestination().isPresent()) {
                        GraphEdgeSchema.End destination = edgeSchema.getDestination().get();
                        // use label destination edge redundancy
                        //TODO: handle singular case
                        if (destination.getEdgeRedundancy().isPresent()) {
                            GraphEdgeRedundancy edgeRedundancy = destination.getEdgeRedundancy().get();
                            // transform each
                            for (HasContainer hasContainer : hasContainers) {
                                Optional<String> redundantProperty = edgeRedundancy.getRedundantPropertyName(hasContainer.getKey());
                                if (redundantProperty.isPresent()) {
                                    newHasContainers.add(new HasContainer(redundantProperty.get(), hasContainer.getPredicate()));
                                } else {
                                    //TODO: no redundant data for property. should we do something?
                                }
                            }
                        }
                    }
                }
            }
        }

        return newHasContainers;
    }

    //region private fields
    private final GraphElementSchemaProvider schemaProvider;
    //endregion
}
