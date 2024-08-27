package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class TimeseriesFieldAnnotation {

  public static final String ANNOTATION_NAME = "TimeseriesField";

  String statName;
  AggregationType aggregationType;

  @Nonnull
  public static TimeseriesFieldAnnotation fromPegasusAnnotationObject(
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
    final Optional<String> statName = AnnotationUtils.getField(map, "name", String.class);
    final Optional<String> aggregationType =
        AnnotationUtils.getField(map, "aggregationType", String.class);

    return new TimeseriesFieldAnnotation(
        statName.orElse(schemaFieldName),
        aggregationType.map(AggregationType::valueOf).orElse(AggregationType.LATEST));
  }

  public enum AggregationType {
    LATEST,
    SUM
  }
}
