package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Simple object representation of the @Aspect annotation metadata.
 */
@Value
public class AspectAnnotation {

  public static final String ANNOTATION_NAME = "Aspect";
  public static final String NAME_FIELD = "name";
  private static final String TYPE_FIELD = "type";
  private static final String IS_KEY_FIELD = "isKey";
  private static final String TIMESERIES_TYPE = "timeseries";

  String name;
  boolean isTimeseries;

  @Nonnull
  public static AspectAnnotation fromSchemaProperty(
      @Nonnull final Object annotationObj,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME,
              context
          ));
    }
    final Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, NAME_FIELD, String.class);
    if (!name.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validated @%s annotation declared at %s: missing '%s' property",
              ANNOTATION_NAME,
              context,
              NAME_FIELD
          ));
    }

    final Optional<String> type = AnnotationUtils.getField(map, TYPE_FIELD, String.class);
    boolean isTimeseries = type.isPresent() && type.get().equals(TIMESERIES_TYPE);

    return new AspectAnnotation(name.get(), isTimeseries);
  }
}
