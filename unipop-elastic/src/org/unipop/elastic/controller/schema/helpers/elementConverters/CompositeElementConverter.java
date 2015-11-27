package org.unipop.elastic.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

/**
 * Created by Roman on 3/21/2015.
 */
public class CompositeElementConverter<TElementSource, TElementDest> implements ElementConverter<TElementSource, TElementDest> {
    public enum Mode {
        First,
        All
    }

    //region Constructor
    public CompositeElementConverter(Mode mode, ElementConverter<TElementSource, TElementDest>... converters) {
        this.mode = mode;
        this.converters = Arrays.asList(converters);
    }
    //endregion

    //region SearchHitElementConverter Implementation
    @Override
    public boolean canConvert(TElementSource element) {
        for(ElementConverter<TElementSource, TElementDest> converter : converters) {
            if (converter.canConvert(element)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Iterable<TElementDest> convert(TElementSource element) {
        switch (this.mode) {
            case First:
                for(ElementConverter<TElementSource, TElementDest> converter : converters) {
                    if (converter.canConvert(element)) {
                        return converter.convert(element);
                    }
                }
                return new ArrayList<>();

            case All:
                ArrayList<TElementDest> elements = new ArrayList<>();
                for(ElementConverter<TElementSource, TElementDest> converter : converters) {
                    if (converter.canConvert(element)) {
                        for(TElementDest convertedElement : converter.convert(element)) {
                            elements.add(convertedElement);
                        }
                    }
                }
                return elements;
        }

        return new ArrayList<>();
    }

    //endregion

    //region Fields
    private Iterable<ElementConverter<TElementSource, TElementDest>> converters;
    private Mode mode;
    //endregion
}
