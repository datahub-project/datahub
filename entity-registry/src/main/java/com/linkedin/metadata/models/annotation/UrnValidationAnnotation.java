package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class UrnValidationAnnotation {
  public static final String ANNOTATION_NAME = "UrnValidation";
  boolean exist;
  boolean strict;
  List<String> entityTypes;

  @Nonnull
  public static UrnValidationAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map<String, ?> map = (Map<String, ?>) annotationObj;
    final Optional<Boolean> exist = AnnotationUtils.getField(map, "exist", Boolean.class);
    final Optional<Boolean> strict = AnnotationUtils.getField(map, "strict", Boolean.class);
    final List<String> entityTypes = AnnotationUtils.getFieldList(map, "entityTypes", String.class);

    return new UrnValidationAnnotation(exist.orElse(true), strict.orElse(true), entityTypes);
  }
}
