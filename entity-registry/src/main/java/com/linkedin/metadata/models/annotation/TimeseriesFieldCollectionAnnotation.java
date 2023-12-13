package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class TimeseriesFieldCollectionAnnotation {
  public static final String ANNOTATION_NAME = "TimeseriesFieldCollection";

  String collectionName;
  String key;

  @Nonnull
  public static TimeseriesFieldCollectionAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> collectionName = AnnotationUtils.getField(map, "name", String.class);
    final Optional<String> key = AnnotationUtils.getField(map, "key", String.class);
    if (!key.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: 'key' field is required",
              ANNOTATION_NAME, context));
    }

    return new TimeseriesFieldCollectionAnnotation(
        collectionName.orElse(schemaFieldName), key.get());
  }
}
