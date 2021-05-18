package com.linkedin.metadata.models.annotation;

import com.linkedin.metadata.models.ModelValidationException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Simple object representation of the @Entity annotation metadata.
 */
@Value
public class EntityAnnotation {

  public static final String ANNOTATION_NAME = "Entity";
  private static final String NAME_FIELD = "name";
  private static final String SEARCHABLE_FIELD = "searchable";
  private static final String BROWSABLE_FIELD = "browsable";

  String name;
  boolean searchable;
  boolean browsable;

  public static EntityAnnotation fromSchemaProperty(
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

    Map map = (Map) annotationObj;
    final Optional<String> name = AnnotationUtils.getField(map, NAME_FIELD, String.class);
    if (!name.isPresent()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field '%s'. Expected type String",
              ANNOTATION_NAME,
              context,
              NAME_FIELD
          ));
    }

    final Optional<Boolean> searchable = AnnotationUtils.getField(map, SEARCHABLE_FIELD, Boolean.class);
    final Optional<Boolean> browsable = AnnotationUtils.getField(map, BROWSABLE_FIELD, Boolean.class);

    return new EntityAnnotation(name.get(), searchable.orElse(false), browsable.orElse(false));
  }
}
