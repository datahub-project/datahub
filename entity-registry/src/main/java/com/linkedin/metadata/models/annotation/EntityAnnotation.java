package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

/** Simple object representation of the @Entity annotation metadata. */
@Value
public class EntityAnnotation {

  public static final String ANNOTATION_NAME = "Entity";
  private static final String NAME_FIELD = "name";
  private static final String KEY_ASPECT_FIELD = "keyAspect";

  String name;
  String keyAspect;

  @Nonnull
  public static EntityAnnotation fromSchemaProperty(
      @Nonnull final Object annotationObj, @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;

    final Optional<String> name = AnnotationUtils.getField(map, NAME_FIELD, String.class);
    final Optional<String> keyAspect =
        AnnotationUtils.getField(map, KEY_ASPECT_FIELD, String.class);

    if (!name.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field '%s'. Expected type String",
              ANNOTATION_NAME, context, NAME_FIELD));
    }
    if (!keyAspect.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field '%s'. Expected type String",
              ANNOTATION_NAME, context, KEY_ASPECT_FIELD));
    }

    return new EntityAnnotation(name.get(), keyAspect.get());
  }
}
