package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

/** Visibility flags for {@code @SystemEntity} and {@code @System} annotations. */
@Value
public class SystemDataVisibility {

  private static final String ALLOW_READ_FIELD = "allowRead";
  private static final String ALLOW_EXISTS_FIELD = "allowExists";

  /** Whether the annotation is present on the PDL record. */
  boolean present;

  /** When present, whether READ is eligible for policy-engine evaluation. */
  boolean allowRead;

  /**
   * When present, whether EXISTS is eligible for policy-engine evaluation. When {@link #allowRead}
   * is true, exists eligibility is implied and this flag is not required.
   */
  boolean allowExists;

  @Nonnull
  public static SystemDataVisibility absent() {
    return new SystemDataVisibility(false, false, false);
  }

  @Nonnull
  public static SystemDataVisibility fullyHidden() {
    return new SystemDataVisibility(true, false, false);
  }

  @Nonnull
  public static SystemDataVisibility fromSchemaProperty(
      @Nonnull final Object annotationObj,
      @Nonnull final String annotationName,
      @Nonnull final String context) {
    if (annotationObj instanceof Boolean) {
      if (Boolean.TRUE.equals(annotationObj)) {
        return fullyHidden();
      }
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: boolean shorthand must be true",
              annotationName, context));
    }
    if (annotationObj instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) annotationObj;
      final boolean allowRead =
          AnnotationUtils.getField(map, ALLOW_READ_FIELD, Boolean.class).orElse(false);
      final boolean allowExists =
          AnnotationUtils.getField(map, ALLOW_EXISTS_FIELD, Boolean.class).orElse(false);
      return new SystemDataVisibility(true, allowRead, allowExists);
    }
    throw new ModelValidationException(
        String.format(
            "Failed to validate @%s annotation declared at %s: expected boolean or map",
            annotationName, context));
  }

  @Nonnull
  public Optional<SystemDataVisibility> toOptional() {
    return present ? Optional.of(this) : Optional.empty();
  }
}
