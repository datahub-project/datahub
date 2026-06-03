package com.linkedin.metadata.models.annotation;

import javax.annotation.Nonnull;
import lombok.Value;

/** Parsed {@code @SystemEntity} annotation on a key aspect PDL record. */
@Value
public class SystemEntityAnnotation {

  public static final String ANNOTATION_NAME = "SystemEntity";

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
  public static SystemEntityAnnotation fromSchemaProperty(
      @Nonnull final Object annotationObj, @Nonnull final String context) {
    return new SystemEntityAnnotation(
        SystemDataVisibility.fromSchemaProperty(annotationObj, ANNOTATION_NAME, context));
  }

  @Nonnull
  public static SystemEntityAnnotation absent() {
    return new SystemEntityAnnotation(SystemDataVisibility.absent());
  }
}
