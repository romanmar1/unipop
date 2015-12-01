package org.unipop.elastic.promise;

import org.unipop.elastic.controller.promise.schemaProviders.GraphPromiseEdgeSchema;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * Created by Karni on 11/25/2015.
 */
@SuppressWarnings("Duplicates")
public class ModernSimpleGraphElementSchemaProvider implements GraphElementSchemaProvider {
    //region Constructor
    public ModernSimpleGraphElementSchemaProvider(String indexName) {
        this.indexName = indexName;
    }
    //endregion

    //region GraphElementSchemaProvider Implementation
    @Override
    public Optional<GraphVertexSchema> getVertexSchema(String type) {
        switch (type) {
            case "person": return Optional.of(getPersonVertexSchema());
            case "software": return Optional.of(getSoftwareVertexSchema());
            default: return null;
        }
    }

    @Override
    public Optional<GraphEdgeSchema> getEdgeSchema(String type, Optional<String> sourceType, Optional<String> destinationType) {
        switch (type) {
            case "knows":
                return Optional.of(getPersonKnowsPerson());
            case "created":
                if (sourceType.isPresent()) {
                    switch (sourceType.get()) {
                        case "person":
                            return Optional.of(getPersonCreatedSoftware());
                        case "software":
                            return Optional.of(getSoftwareCreatedByPerson());
                        default:
                            return null;
                    }
                } else if (destinationType.isPresent()) {
                    switch (sourceType.get()) {
                        case "person":
                            return Optional.of(getSoftwareCreatedByPerson());
                        case "software":
                            return Optional.of(getPersonCreatedSoftware());
                        default:
                            return null;
                    }
                } else {
                    return Optional.empty();
                }
            case "promise": return Optional.of(getPromiseEdgeSchema());
            default: return Optional.empty();
        }
    }

    @Override
    public Optional<Iterable<GraphEdgeSchema>> getEdgeSchemas(String type) {
        switch (type) {
            case "knows": return Optional.of(Arrays.asList(getPersonKnowsPerson()));
            case "created": return Optional.of(Arrays.asList(getPersonCreatedSoftware(), getSoftwareCreatedByPerson()));
            default: return null;
        }
    }

    @Override
    public Iterable<String> getVertexTypes() {
        return Arrays.asList(
                "person",
                "software"
        );
    }

    @Override
    public Iterable<String> getEdgeTypes() {
        return Arrays.asList(
                "knows",
                "created"
        );
    }
    //endregion

    //region Private Methods
    public GraphVertexSchema getPersonVertexSchema() {
        return new GraphVertexSchema() {
            @Override
            public String getType() {
                return "person";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(indexName);
            }
        };
    }

    public GraphVertexSchema getSoftwareVertexSchema() {
        return new GraphVertexSchema() {
            @Override
            public String getType() {
                return "software";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(indexName);
            }
        };
    }

    public GraphEdgeSchema getPersonKnowsPerson() {
        return new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "sourceId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("source."));
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "destinationId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("destination."));
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.of(new Direction() {
                    @Override
                    public String getField() {
                        return "direction";
                    }

                    @Override
                    public Object getInValue() {
                        return "in";
                    }

                    @Override
                    public Object getOutValue() {
                        return "out";
                    }
                });
            }

            @Override
            public String getType() {
                return "knows";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(indexName);
            }
        };
    }

    public GraphEdgeSchema getPersonCreatedSoftware() {
        return new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "sourceId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("source."));
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "destinationId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("destination."));
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.of(new Direction() {
                    @Override
                    public String getField() {
                        return "direction";
                    }

                    @Override
                    public Object getInValue() {
                        return "in";
                    }

                    @Override
                    public Object getOutValue() {
                        return "out";
                    }
                });
            }

            @Override
            public String getType() {
                return "created";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(indexName);
            }
        };
    }

    public GraphEdgeSchema getSoftwareCreatedByPerson() {
        return new GraphEdgeSchema() {
            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "sourceId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("software");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("source."));
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return "destinationId";
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("person");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.of(new PrefixedEdgeRedundancy("destination."));
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.of(new Direction() {
                    @Override
                    public String getField() {
                        return "direction";
                    }

                    @Override
                    public Object getInValue() {
                        return "in";
                    }

                    @Override
                    public Object getOutValue() {
                        return "out";
                    }
                });
            }

            @Override
            public String getType() {
                return "created";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Arrays.asList(indexName);
            }
        };
    }

    public GraphPromiseEdgeSchema getPromiseEdgeSchema() {
        return new GraphPromiseEdgeSchema() {
            @Override
            public Iterable<Property> getProperties() {
                return Arrays.asList(new Property() {
                    @Override
                    public String getName() {
                        return "count";
                    }
                });
            }

            @Override
            public Optional<End> getSource() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return null;
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("promise");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.empty();
                    }
                });
            }

            @Override
            public Optional<End> getDestination() {
                return Optional.of(new End() {
                    @Override
                    public String getIdField() {
                        return null;
                    }

                    @Override
                    public Optional<String> getType() {
                        return Optional.of("promise");
                    }

                    @Override
                    public Optional<GraphEdgeRedundancy> getEdgeRedundancy() {
                        return Optional.empty();
                    }
                });
            }

            @Override
            public Optional<Direction> getDirection() {
                return Optional.empty();
            }

            @Override
            public String getType() {
                return "promise";
            }

            @Override
            public Optional<GraphElementRouting> getRouting() {
                return Optional.empty();
            }

            @Override
            public Iterable<String> getIndices() {
                return Collections.emptyList();
            }
        };
    }
    //endregion

    //region Fields
    private String indexName;
    //endregion
}
