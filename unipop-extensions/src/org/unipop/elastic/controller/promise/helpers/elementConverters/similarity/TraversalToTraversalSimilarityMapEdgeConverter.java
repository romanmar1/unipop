package org.unipop.elastic.controller.promise.helpers.elementConverters.similarity;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.lambda.Seq;
import org.unipop.elastic.controller.promise.PromiseEdge;
import org.unipop.elastic.controller.promise.PromiseVertex;
import org.unipop.elastic.controller.promise.TraversalPromise;
import org.unipop.elastic.controller.promise.helpers.PromiseStrings;
import org.unipop.elastic.controller.promise.helpers.queryAppenders.helpers.provider.TraversalIdProvider;
import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseVertexSchema;
import org.unipop.elastic.controller.schema.helpers.MapHelper;
import org.unipop.elastic.controller.schema.helpers.TraversalVisitor;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by Roman on 12/2/2015.
 */
public class TraversalToTraversalSimilarityMapEdgeConverter extends GraphPromiseSimilarityEdgeConverterBase<Map<String, Object>> {
    //region Constructor
    public TraversalToTraversalSimilarityMapEdgeConverter(
            UniGraph graph,
            Direction direction,
            Iterable<TraversalPromise> bulkTraversalPromises,
            GraphElementSchemaProvider schemaProvider,
            TraversalIdProvider<String> traversalIdProvider) {
        super(graph, direction, schemaProvider, traversalIdProvider);

        this.bulkTraversalPromises = Seq.seq(bulkTraversalPromises).groupBy(traversalPromise -> traversalPromise.getId());
    }
    //endregion

    //region ElementConverter Implementation
    @Override
    public boolean canConvert(Map<String, Object> map) {
        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        if (bulkTraversalPromisesMap == null || bulkTraversalPromisesMap.size() == 0) {
            return false;
        }

        return true;
    }

    @Override
    public Iterable<Element> convert(Map<String, Object> map) {
        ArrayList<Element> edges = new ArrayList<>();

        Optional<GraphPromiseVertexSchema> graphPromiseVertexSchema = getGraphPromiseVertexSchema();

        Map<String, Object> bulkTraversalPromisesMap = MapHelper.value(map, PromiseStrings.BULK_TRAVERSAL_PROMISES);
        for (Map.Entry<String, Object> traversalPromiseEntry : bulkTraversalPromisesMap.entrySet()) {
            Map<String, Object> traversalPromisePropertiesMap = (Map<String, Object>)traversalPromiseEntry.getValue();

            Map<String, List<String>> propertyValues = new HashMap<>();
            for(String similarityProperty : graphPromiseVertexSchema.get().getSimilarity().getSimilarityProperties()) {
                Map<String, Object> propertyValuesMap = (Map<String, Object>)traversalPromisePropertiesMap.get(similarityProperty);
                if (propertyValuesMap == null) {
                    continue;
                }

                propertyValues.put(similarityProperty, Seq.seq(propertyValuesMap.keySet()).filter(property -> !property.equals("count")).toList());
            }

            TraversalPromise traversalPromiseToExpand = bulkTraversalPromises.get(traversalPromiseEntry.getKey()).get(0);
            Traversal clonedTraversal = traversalPromiseToExpand.getTraversal().asAdmin().clone();

            Visitor visitor = new Visitor(propertyValues);
            visitor.visit(clonedTraversal);

            for(Map.Entry<String, List<String>> propertyValuesEntry : propertyValues.entrySet()) {
                if (visitor.getHandledProperties().contains(propertyValuesEntry.getKey()) || propertyValuesEntry.getValue().isEmpty()) {
                    continue;
                }

                if (OrStep.class.isAssignableFrom(clonedTraversal.asAdmin().getSteps().get(0).getClass())) {
                    ((OrStep)clonedTraversal.asAdmin().getSteps().get(0))
                            .addLocalChild(__.has(propertyValuesEntry.getKey(), P.within(propertyValuesEntry.getValue())).asAdmin());
                } else {
                    clonedTraversal = __.or(clonedTraversal, __.has(propertyValuesEntry.getKey(), P.within(propertyValuesEntry.getValue())));
                }
            }

            PromiseVertex traversalPromiseVertex = new PromiseVertex(traversalPromiseToExpand, this.graph);
            PromiseVertex similarTraversalPromiseVertex = traversalPromiseToExpand.getIsStrongId() ?
                     new PromiseVertex(new TraversalPromise(traversalPromiseToExpand.getId(), clonedTraversal), this.graph) :
                     new PromiseVertex(new TraversalPromise(this.traversalIdProvider.getId(clonedTraversal), clonedTraversal), this.graph);

            switch (this.direction) {
                case OUT:
                    edges.add(new PromiseEdge(getEdgeId(traversalPromiseVertex, similarTraversalPromiseVertex), traversalPromiseVertex, similarTraversalPromiseVertex, null, this.graph));
                    break;

                case IN:
                    edges.add(new PromiseEdge(getEdgeId(similarTraversalPromiseVertex, traversalPromiseVertex), similarTraversalPromiseVertex, traversalPromiseVertex, null, this.graph));
                    break;

                case BOTH:
                    edges.add(new PromiseEdge(getEdgeId(traversalPromiseVertex, similarTraversalPromiseVertex), traversalPromiseVertex, similarTraversalPromiseVertex, null, this.graph));
                    edges.add(new PromiseEdge(getEdgeId(similarTraversalPromiseVertex, traversalPromiseVertex), similarTraversalPromiseVertex, traversalPromiseVertex, null, this.graph));
                    break;
            }
        }

        return edges;
    }
    //endregion

    //region Fields
    private Map<Object, List<TraversalPromise>> bulkTraversalPromises;
    //endregion

    //region Visitor
    private class Visitor extends TraversalVisitor {
        //region Constructor
        public Visitor(Map<String, List<String>> propertyValues) {
            this.propertyValues = propertyValues;
            this.handledProperties = new HashSet<>();
        }
        //endregion

        //region Override Methods
        @Override
        protected void visitHasStep(HasStep hasStep) {
            ArrayList<HasContainer> newHasContainers = new ArrayList<>();
            boolean hasContainerWasModified = false;
            for(Object obj : hasStep.getHasContainers()) {
                HasContainer hasContainer = (HasContainer)obj;
                List<String> similarityPropertyValues = this.propertyValues.get(hasContainer.getKey());

                handledProperties.add(hasContainer.getKey());

                if (similarityPropertyValues == null || similarityPropertyValues.isEmpty()) {
                    newHasContainers.add(hasContainer);
                    continue;
                }

                if (similarityPropertyValues.size() > 1) {
                    newHasContainers.add(new HasContainer(hasContainer.getKey(), P.within(similarityPropertyValues)));
                    hasContainerWasModified = true;
                }
            }

            if (hasContainerWasModified) {
                HasStep newHasStep = new HasStep(hasStep.getTraversal(), Seq.seq(newHasContainers).toArray(HasContainer[]::new));
                TraversalHelper.insertAfterStep(newHasStep, hasStep, hasStep.getTraversal());
                newHasStep.getTraversal().removeStep(hasStep);
            }
        }
        //endregion


        //region Properties
        public Set<String> getHandledProperties() {
            return handledProperties;
        }
        //endregion

        //region Fields
        Map<String, List<String>> propertyValues;
        Set<String> handledProperties;
        //endregion
    }
    //endregion
}
