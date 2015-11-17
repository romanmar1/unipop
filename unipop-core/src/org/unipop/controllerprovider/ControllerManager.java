package org.unipop.controllerprovider;


import org.apache.commons.configuration.Configuration;
import org.unipop.elastic.controller.EdgeController;
import org.unipop.elastic.controller.VertexController;
import org.unipop.structure.UniGraph;

public interface ControllerManager extends VertexController, EdgeController {

    void init(UniGraph graph, Configuration configuration) throws Exception;
    void commit();
    void close();
}
