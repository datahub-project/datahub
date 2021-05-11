package com.linkedin.metadata.models.annotation;

import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.Optional;

@UtilityClass
public class AnnotationUtils {
    <T> Optional<T> getField(Map fieldMap, String fieldName, Class<T> fieldType) {
        if (fieldMap.containsKey(fieldName) && fieldType.isAssignableFrom(fieldMap.get(fieldName).getClass())) {
            return Optional.of(fieldType.cast(fieldMap.get(fieldName)));
        }
        return Optional.empty();
    }
}
