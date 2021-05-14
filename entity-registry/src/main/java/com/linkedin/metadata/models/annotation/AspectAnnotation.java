package com.linkedin.metadata.models.annotation;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Simple object representation of the @Aspect annotation metadata.
 */
@Value
public class AspectAnnotation {

  String name;
  Boolean isKey;

  public static AspectAnnotation fromSchemaProperty(@Nonnull final Object annotationObj) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new IllegalArgumentException(
          "Failed to validate @Aspect annotation object: Invalid value type provided (Expected Map)");
    }
    Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, "name", String.class);
    if (!name.isPresent()) {
      throw new IllegalArgumentException("Failed to validated @Aspect annotation object: missing 'name' property");
    }
    final Optional<Boolean> isKey = AnnotationUtils.getField(map, "isKey", Boolean.class);

    return new AspectAnnotation(name.get(), isKey.orElse(false));
  }
}
