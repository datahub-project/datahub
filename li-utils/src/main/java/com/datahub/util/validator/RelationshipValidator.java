package com.datahub.util.validator;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.Value;

public class RelationshipValidator {

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED =
      ConcurrentHashMap.newKeySet();

  // A cache of validated classes
  private static final Set<Class<? extends UnionTemplate>> UNION_VALIDATED =
      ConcurrentHashMap.newKeySet();

  @Value
  private static class Pair {
    String source;
    String destination;
  }

  private RelationshipValidator() {
    // Util class
  }

  /**
   * Validates a specific relationship model defined in com.linkedin.metadata.relationship.
   *
   * @param schema schema for the model
   */
  public static void validateRelationshipSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(
        schema, field -> ValidationUtils.isValidUrnField(field, "source"))) {
      ValidationUtils.invalidSchema(
          "Relationship '%s' must contain an non-optional 'source' field of URN type", className);
    }

    if (!ValidationUtils.schemaHasExactlyOneSuchField(
        schema, field -> ValidationUtils.isValidUrnField(field, "destination"))) {
      ValidationUtils.invalidSchema(
          "Relationship '%s' must contain an non-optional 'destination' field of URN type",
          className);
    }

    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES)
        .forEach(
            field -> {
              ValidationUtils.invalidSchema(
                  "Relationship '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
                  className, field.getName(), field.getType().getType());
            });
  }

  /**
   * Similar to {@link #validateRelationshipSchema(RecordDataSchema)} but take a {@link Class}
   * instead and caches results.
   */
  public static void validateRelationshipSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateRelationshipSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }

  /**
   * Similar to {@link #validateRelationshipUnionSchema(UnionDataSchema, String)} but take a {@link
   * Class} instead and caches results.
   */
  public static void validateRelationshipUnionSchema(
      @Nonnull Class<? extends UnionTemplate> clazz) {
    if (UNION_VALIDATED.contains(clazz)) {
      return;
    }

    validateRelationshipUnionSchema(
        ValidationUtils.getUnionSchema(clazz), clazz.getCanonicalName());
    UNION_VALIDATED.add(clazz);
  }

  /**
   * Validates the union of relationship model defined in com.linkedin.metadata.relationship.
   *
   * @param schema schema for the model
   */
  public static void validateRelationshipUnionSchema(
      @Nonnull UnionDataSchema schema, @Nonnull String relationshipClassName) {

    if (!ValidationUtils.isUnionWithOnlyComplexMembers(schema)) {
      ValidationUtils.invalidSchema(
          "Relationship '%s' must be a union containing only record type members",
          relationshipClassName);
    }
  }
}
