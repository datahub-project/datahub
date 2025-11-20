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
  public static final String DEFAULT_SEARCH_GROUP = "default";
  private static final String NAME_FIELD = "name";
  private static final String KEY_ASPECT_FIELD = "keyAspect";
  private static final String SEARCH_GROUP_FIELD = "searchGroup";

  String name;
  String keyAspect;
  String searchGroup;

  public EntityAnnotation(String name, String keyAspect) {
    this(name, keyAspect, DEFAULT_SEARCH_GROUP);
  }

  public EntityAnnotation(String name, String keyAspect, String searchGroup) {
    this.name = name;
    this.keyAspect = keyAspect;
    this.searchGroup = searchGroup;
  }

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

    // Get searchGroup with default value if not present
    final String searchGroup =
        AnnotationUtils.getField(map, SEARCH_GROUP_FIELD, String.class)
            .orElse(DEFAULT_SEARCH_GROUP);

    // Validate searchGroup
    validateElasticsearchIndexName(searchGroup, context);

    return new EntityAnnotation(name.get(), keyAspect.get(), searchGroup);
  }

  /** Validates that searchGroup follows Elasticsearch index naming conventions */
  private static void validateElasticsearchIndexName(String indexName, String context) {
    if (indexName == null || indexName.trim().isEmpty()) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: searchGroup cannot be null or empty",
              ANNOTATION_NAME, context));
    }

    // Check length (Elasticsearch limit is 255 bytes)
    if (indexName.length() > 255) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: searchGroup cannot be longer than 255 characters, but was %d characters: '%s'",
              ANNOTATION_NAME, context, indexName.length(), indexName));
    }

    // Check if it's a reserved name
    if (".".equals(indexName) || "..".equals(indexName)) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: searchGroup cannot be '.' or '..', but was '%s'",
              ANNOTATION_NAME, context, indexName));
    }

    // Check if it starts with invalid characters
    if (indexName.startsWith("-") || indexName.startsWith("_") || indexName.startsWith("+")) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: searchGroup cannot start with '-', '_', or '+', but was '%s'",
              ANNOTATION_NAME, context, indexName));
    }

    // Check for uppercase characters (must be lowercase)
    if (!indexName.equals(indexName.toLowerCase())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: searchGroup must be lowercase, but was '%s'",
              ANNOTATION_NAME, context, indexName));
    }

    // Check for invalid characters
    String invalidChars = "\\/*?\"<>| ,#";
    for (char c : invalidChars.toCharArray()) {
      if (indexName.indexOf(c) != -1) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchGroup cannot contain '%c', but was '%s'",
                ANNOTATION_NAME, context, c, indexName));
      }
    }
  }
}
