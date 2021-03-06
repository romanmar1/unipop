package org.unipop.jdbc.controller.vertex;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class SqlVertexController implements VertexController {

    private final DSLContext dslContext;
    private final UniGraph graph;
    private final String tableName;
    private final VertexMapper vertexMapper;
    int idCount = 10000;

    public SqlVertexController(String tableName, UniGraph graph, Connection conn) {
        this.graph = graph;
        this.tableName = tableName;
        dslContext = DSL.using(conn, SQLDialect.DEFAULT);
        vertexMapper = new VertexMapper();
        //dslContext.settings().setRenderNameStyle(RenderNameStyle.AS_IS);
    }

    public DSLContext getContext() {
        return dslContext;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        predicates.hasContainers.forEach(hasContainer -> select.where(JooqHelper.createCondition(hasContainer)));
        select.limit(0, predicates.limitHigh < Long.MAX_VALUE ? (int)predicates.limitHigh : Integer.MAX_VALUE);
        return select.fetch(vertexMapper).iterator();
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return dslContext.select().from(tableName).where(field("id").eq(vertexId)).fetchOne(vertexMapper);
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        if(id == null) id = idCount++; //TODO: make this smarter...
        properties.putIfAbsent("id", id);

        dslContext.insertInto(table(tableName), CollectionUtils.collect(properties.keySet(), DSL::field))
                .values(properties.values()).execute();

        return new SqlVertex(id, label, properties, this, graph);
    }

    private SqlVertexController self = this;
    private class VertexMapper implements RecordMapper<Record, BaseVertex> {

        @Override
        public BaseVertex map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            return new SqlVertex(stringObjectMap.get("id"), tableName.toLowerCase(), stringObjectMap, self, graph);
        }
    }
}
