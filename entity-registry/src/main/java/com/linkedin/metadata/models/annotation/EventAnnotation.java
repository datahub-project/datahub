package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

/** An annotation associated with a DataHub Event. */
@Value
public class EventAnnotation {

  public static final String ANNOTATION_NAME = "Event";
  private static final String NAME_FIELD = "name";

  String name;

  @Nonnull
  public static EventAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj, @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, NAME_FIELD, String.class);
    if (!name.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation at %s: Invalid field '%s'. Expected type String",
              ANNOTATION_NAME, context, NAME_FIELD));
    }
    return new EventAnnotation(name.get());
  }
}
