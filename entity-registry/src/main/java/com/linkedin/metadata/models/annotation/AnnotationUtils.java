package com.linkedin.metadata.models.annotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  <T> List<T> getFieldList(
      final Map<String, ?> fieldMap, final String fieldName, final Class<T> itemType) {
    Object value = fieldMap.get(fieldName);
    if (!(value instanceof List<?>)) {
      return Collections.emptyList();
    }

    List<?> list = (List<?>) value;
    List<T> result = new ArrayList<>();

    for (Object item : list) {
      if (itemType.isInstance(item)) {
        result.add(itemType.cast(item));
      }
    }

    return Collections.unmodifiableList(result);
  }
}
