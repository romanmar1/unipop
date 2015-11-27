package org.unipop.elastic.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Created by Roman on 3/16/2015.
 */
public interface ElementConverter<TElementSource, TElementDest> {
    boolean canConvert(TElementSource source);
    Iterable<TElementDest> convert(TElementSource source);
}
