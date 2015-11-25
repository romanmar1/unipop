package org.unipop.elastic.helpers;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.HppcMaps;
import org.unipop.elastic.controller.schema.SchemaEdge;
import org.unipop.elastic.controller.schema.SchemaVertex;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.structure.BaseElement;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ElasticSchemaGraphMutations extends ElasticMutations {
    //region Constructor
    public ElasticSchemaGraphMutations(GraphElementSchemaProvider schemaProvider, Boolean bulk, Client client, TimingAccessor timing) {
        super(bulk, client, timing);
        this.schemaProvider = schemaProvider;
    }
    //endregion

    //Override Methods
    @SuppressWarnings("Duplicates")
    @Override
    public void addElement(BaseElement element, String index, String routing,  boolean create) {
        // Special implementation is needed just in case of dual edges. In all other cases - use default
        if (!SchemaEdge.class.isAssignableFrom(element.getClass())) {
            super.addElement(element, index, routing, create);
            return;
        }

        SchemaEdge schemaEdge = (SchemaEdge)element;
        Optional<GraphEdgeSchema> optionalEdgeSchema = schemaProvider.getEdgeSchema(
                schemaEdge.label(),
                Optional.of(schemaEdge.outVertex().label()),
                Optional.of(schemaEdge.inVertex().label()));

        // no edge schema or no edge direction - use default
        if (!optionalEdgeSchema.isPresent() || !optionalEdgeSchema.get().getDirection().isPresent()) {
            super.addElement(element, index, routing, create);
            return;
        }

        Map<String, Object> inVertexAllFields = ((BaseVertex)schemaEdge.inVertex()).allFields();
        Map<String, Object> outVertexAllFields = ((BaseVertex)schemaEdge.outVertex()).allFields();

        GraphEdgeSchema.Direction originalDirection = optionalEdgeSchema.get().getDirection().get();

        Map<String, Object> originalAllFields = schemaEdge.allFields();
        Object originalDirectionValue = originalAllFields.get(originalDirection.getField());

        Map<String, Object> otherFields = new HashMap<>(originalAllFields);
        Object otherDirectionValue = originalDirection.getInValue() == originalDirectionValue ?
                originalDirection.getOutValue() :
                originalDirection.getInValue();
        otherFields.put(originalDirection.getField(), otherDirectionValue);

        GraphEdgeSchema.End originalSourceEnd;
        GraphEdgeSchema.End originalDestinationEnd;
        if (originalDirectionValue == originalDirection.getOutValue()) {
            originalSourceEnd = optionalEdgeSchema.get().getSource().get();
            originalDestinationEnd = optionalEdgeSchema.get().getDestination().get();
        } else {
            originalSourceEnd = optionalEdgeSchema.get().getDestination().get();
            originalDestinationEnd = optionalEdgeSchema.get().getSource().get();
        }

        outVertexAllFields.entrySet().forEach(entry -> {
            originalAllFields.put(originalSourceEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        outVertexAllFields.entrySet().forEach(entry -> {
            otherFields.put(originalDestinationEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        inVertexAllFields.entrySet().forEach(entry -> {
            originalAllFields.put(originalDestinationEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        inVertexAllFields.entrySet().forEach(entry -> {
            otherFields.put(originalSourceEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });

        otherFields.put(originalSourceEnd.getIdField(), schemaEdge.inVertex().id());
        otherFields.put(originalDestinationEnd.getIdField(), schemaEdge.outVertex().id());

        IndexRequestBuilder indexRequest1 = client.prepareIndex(index, element.label(), element.id().toString())
                .setSource(originalAllFields).setRouting(routing).setCreate(create);
        IndexRequestBuilder indexRequest2 = client.prepareIndex(index, element.label(), element.id().toString() + "_dual")
                .setSource(otherFields).setRouting(routing).setCreate(create);

        isDirty = true;
        indicesToRefresh.add(index);

        if(bulkRequest != null) {
            bulkRequest.add(indexRequest1);
            bulkRequest.add(indexRequest2);
        }
        else {
            indexRequest1.execute().actionGet();
            indexRequest2.execute().actionGet();
        }
        revision++;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void updateElement(BaseElement element, String index, String routing, boolean upsert) throws ExecutionException, InterruptedException {
        // Special implementation is needed just in case of dual edges. In all other cases - use default
        if (!SchemaEdge.class.isAssignableFrom(element.getClass())) {
            super.updateElement(element, index, routing, upsert);
            return;
        }

        SchemaEdge schemaEdge = (SchemaEdge)element;
        Optional<GraphEdgeSchema> optionalEdgeSchema = schemaProvider.getEdgeSchema(
                schemaEdge.label(),
                Optional.of(schemaEdge.outVertex().label()),
                Optional.of(schemaEdge.inVertex().label()));

        // no edge schema or no edge direction - use default
        if (!optionalEdgeSchema.isPresent() || !optionalEdgeSchema.get().getDirection().isPresent()) {
            super.updateElement(element, index, routing, upsert);
            return;
        }

        Map<String, Object> inVertexAllFields = ((BaseVertex)schemaEdge.inVertex()).allFields();
        Map<String, Object> outVertexAllFields = ((BaseVertex)schemaEdge.outVertex()).allFields();

        GraphEdgeSchema.Direction originalDirection = optionalEdgeSchema.get().getDirection().get();

        Map<String, Object> originalAllFields = schemaEdge.allFields();
        Object originalDirectionValue = originalAllFields.get(originalDirection.getField());

        Map<String, Object> otherFields = new HashMap<>(originalAllFields);
        Object otherDirectionValue = originalDirection.getInValue() == originalDirectionValue ?
                originalDirection.getOutValue() :
                originalDirection.getInValue();
        otherFields.put(originalDirection.getField(), otherDirectionValue);

        GraphEdgeSchema.End originalSourceEnd;
        GraphEdgeSchema.End originalDestinationEnd;
        if (originalDirectionValue == originalDirection.getOutValue()) {
            originalSourceEnd = optionalEdgeSchema.get().getSource().get();
            originalDestinationEnd = optionalEdgeSchema.get().getDestination().get();
        } else {
            originalSourceEnd = optionalEdgeSchema.get().getDestination().get();
            originalDestinationEnd = optionalEdgeSchema.get().getSource().get();
        }

        outVertexAllFields.entrySet().forEach(entry -> {
            originalAllFields.put(originalSourceEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        outVertexAllFields.entrySet().forEach(entry -> {
            otherFields.put(originalDestinationEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        inVertexAllFields.entrySet().forEach(entry -> {
            originalAllFields.put(originalDestinationEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });
        inVertexAllFields.entrySet().forEach(entry -> {
            otherFields.put(originalSourceEnd.getEdgeRedundancy().get()
                    .getRedundantPropertyName(entry.getKey()).get(), entry.getValue());
        });

        otherFields.put(originalSourceEnd.getIdField(), schemaEdge.inVertex().id());
        otherFields.put(originalDestinationEnd.getIdField(), schemaEdge.outVertex().id());

        UpdateRequest updateRequest1 = new UpdateRequest(index, element.label(), element.id().toString())
                .doc(originalAllFields).routing(routing);
        UpdateRequest updateRequest2 = new UpdateRequest(index, element.label(), element.id().toString() + "_dual")
                .doc(otherFields).routing(routing);

        isDirty = true;
        indicesToRefresh.add(index);

        if(upsert) {
            updateRequest1.detectNoop(true).docAsUpsert(true);
            updateRequest2.detectNoop(true).docAsUpsert(true);
        }

        if(bulkRequest != null) {
            bulkRequest.add(updateRequest1);
            bulkRequest.add(updateRequest2);
        } else {
            client.update(updateRequest1).actionGet();
            client.update(updateRequest2).actionGet();
        }
        revision++;
    }

    @SuppressWarnings("Duplicates")
    public void deleteElement(BaseElement element, String index, String routing) {
        // Special implementation is needed just in case of dual edges. In all other cases - use default
        if (!SchemaEdge.class.isAssignableFrom(element.getClass())) {
            super.deleteElement(element, index, routing);
            return;
        }

        SchemaEdge schemaEdge = (SchemaEdge)element;
        Optional<GraphEdgeSchema> optionalEdgeSchema = schemaProvider.getEdgeSchema(
                schemaEdge.label(),
                Optional.of(schemaEdge.outVertex().label()),
                Optional.of(schemaEdge.inVertex().label()));

        // no edge schema or no edge direction - use default
        if (!optionalEdgeSchema.isPresent() || !optionalEdgeSchema.get().getDirection().isPresent()) {
            super.deleteElement(element, index, routing);
            return;
        }

        DeleteRequestBuilder deleteRequestBuilder1 = client.prepareDelete(index, element.label(), element.id().toString()).setRouting(routing);
        DeleteRequestBuilder deleteRequestBuilder2 = client.prepareDelete(index, element.label(), element.id().toString() + "_dual").setRouting(routing);

        isDirty = true;
        indicesToRefresh.add(index);

        if(bulkRequest != null) {
            bulkRequest.add(deleteRequestBuilder1);
            bulkRequest.add(deleteRequestBuilder2);
        }
        else {
            deleteRequestBuilder1.execute().actionGet();
            deleteRequestBuilder2.execute().actionGet();
        }
        revision++;
    }
    //endregion

    // region Fields
    private final GraphElementSchemaProvider schemaProvider;
    // endregion
}
