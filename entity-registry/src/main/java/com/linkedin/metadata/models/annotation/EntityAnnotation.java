package com.linkedin.metadata.models.annotation;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Simple object representation of the @Entity annotation metadata.
 */
public class EntityAnnotation {

    private final String _name;
    private final Boolean _searchable;
    private final Boolean _browsable;

    public EntityAnnotation(@Nonnull final String name,
                            @Nonnull final Boolean searchable,
                            @Nonnull final Boolean browsable) {
        _name = name;
        _searchable = searchable;
        _browsable = browsable;
    }

    public String getName() {
        return _name;
    }

    public Boolean isSearchable() {
        return _searchable;
    }

    public Boolean isBrowsable() {
        return _browsable;
    }

    public static EntityAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
        if (Map.class.isAssignableFrom(annotationObj.getClass())) {
            Map map = (Map) annotationObj;
            final Object nameObj = map.get("name");
            final Object searchableObj = map.get("searchable");
            final Object browsableObj = map.get("browsable");
            if (nameObj == null || !String.class.isAssignableFrom(nameObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required @Entity field 'name' field of type String");
            }
            if (searchableObj == null || !Boolean.class.isAssignableFrom(searchableObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required @Entity field 'searchable' field of type Boolean");
            }
            if (browsableObj == null || !Boolean.class.isAssignableFrom(browsableObj.getClass())) {
                throw new IllegalArgumentException("Failed to validate required @Entity field 'browsable' field of type Boolean");
            }
            return new EntityAnnotation((String) nameObj, (Boolean) searchableObj, (Boolean) browsableObj);
        }
        throw new IllegalArgumentException("Failed to validate @Entity annotation object: Invalid value type provided (Expected Map)");
    }
}
