package org.unipop.elastic.helpers;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseElement;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public interface GraphMutations {
    public void addElement(BaseElement element, String index, String routing,  boolean create);

    public void updateElement(BaseElement element, String index, String routing, boolean upsert) throws ExecutionException, InterruptedException;

    public void deleteElement(BaseElement element, String index, String routing);

    public void commit();
}
