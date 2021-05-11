package com.linkedin.metadata.models.annotation;

import lombok.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * Simple object representation of the @Aspect annotation metadata.
 */
@Value
public class AspectAnnotation {

    String name;
    Boolean isKey;

    public static AspectAnnotation fromSchemaProperty(@Nonnull final Object annotationObj, @Nonnull final String fullyQualifiedName) {
        if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
            throw new IllegalArgumentException("Failed to validate @Aspect annotation object: Invalid value type provided (Expected Map)");
        }
        Map map = (Map) annotationObj;
        final Optional<Boolean> isKey = AnnotationUtils.getField(map, "isKey", Boolean.class);
        return new AspectAnnotation(fullyQualifiedName, isKey.orElse(false));
    }
}
