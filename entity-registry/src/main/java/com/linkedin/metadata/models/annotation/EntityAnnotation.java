package com.linkedin.metadata.models.annotation;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Simple object representation of the @Entity annotation metadata.
 */
@Value
public class EntityAnnotation {

  String name;
  boolean searchable;
  boolean browsable;

  public static EntityAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new IllegalArgumentException(
          "Failed to validate @Entity annotation object: Invalid value type provided (Expected Map)");
    }

    Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, "name", String.class);
    if (!name.isPresent()) {
      throw new IllegalArgumentException("Failed to validate required @Entity field 'name' field of type String");
    }

    final Optional<Boolean> searchable = AnnotationUtils.getField(map, "searchable", Boolean.class);
    final Optional<Boolean> browsable = AnnotationUtils.getField(map, "browsable", Boolean.class);

    return new EntityAnnotation(name.get(), searchable.orElse(false), browsable.orElse(false));
  }
}
