package com.linkedin.metadata.models.annotation;

import java.util.Map;
import java.util.Optional;
import lombok.experimental.UtilityClass;

@UtilityClass
public class AnnotationUtils {
  <T> Optional<T> getField(final Map fieldMap, final String fieldName, final Class<T> fieldType) {
    if (fieldMap.containsKey(fieldName)
        && fieldType.isAssignableFrom(fieldMap.get(fieldName).getClass())) {
      return Optional.of(fieldType.cast(fieldMap.get(fieldName)));
    }
    return Optional.empty();
  }
}
