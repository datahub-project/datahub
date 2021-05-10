package com.linkedin.metadata.models.annotation;

import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * Simple object representation of the @Entity annotation metadata.
 */
@Value
public class EntityAnnotation {

    String _name;
    boolean _searchable;
    boolean _browsable;

    public static EntityAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
        if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
            throw new IllegalArgumentException("Failed to validate @Entity annotation object: Invalid value type provided (Expected Map)");
        }

        Map map = (Map) annotationObj;
        final Optional<String> name = AnnotationUtils.getField(map, "name", String.class);
        if (!name.isPresent()) {
            throw new IllegalArgumentException("Failed to validate required @Entity field 'name' field of type String");
        }

        final Optional<Boolean> searchable = AnnotationUtils.getField(map, "searchable", Boolean.class);
        if (!searchable.isPresent()) {
            throw new IllegalArgumentException("Failed to validate required @Entity field 'searchable' field of type Boolean");
        }

        final Optional<Boolean> browsable = AnnotationUtils.getField(map, "browsable", Boolean.class);
        if (!browsable.isPresent()) {
            throw new IllegalArgumentException("Failed to validate required @Entity field 'browsable' field of type Boolean");
        }
        return new EntityAnnotation(name.get(), searchable.get(), browsable.get());
    }
}
