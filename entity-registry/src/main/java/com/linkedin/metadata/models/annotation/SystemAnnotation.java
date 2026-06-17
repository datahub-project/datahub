package com.linkedin.metadata.models.annotation;

import javax.annotation.Nonnull;
import lombok.Value;

/** Parsed {@code @System} annotation on an aspect PDL record (system entities only). */
@Value
public class SystemAnnotation {

  public static final String ANNOTATION_NAME = "System";

  @Nonnull SystemDataVisibility visibility;

  public boolean isPresent() {
    return visibility.isPresent();
  }

  public boolean isAllowRead() {
    return visibility.isAllowRead();
  }

  public boolean isAllowExists() {
    return visibility.isAllowExists();
  }

  @Nonnull
  public static SystemAnnotation fromSchemaProperty(
      @Nonnull final Object annotationObj, @Nonnull final String context) {
    return new SystemAnnotation(
        SystemDataVisibility.fromSchemaProperty(annotationObj, ANNOTATION_NAME, context));
  }

  @Nonnull
  public static SystemAnnotation absent() {
    return new SystemAnnotation(SystemDataVisibility.absent());
  }
}
