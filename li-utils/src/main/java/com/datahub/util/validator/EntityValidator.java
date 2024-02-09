package com.datahub.util.validator;

import com.datahub.util.exception.InvalidSchemaException;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/** Utility class to validate entity schemas. */
public final class EntityValidator {

  // Allowed non-optional fields. All other fields must be optional.
  private static final Set<String> NON_OPTIONAL_FIELDS =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("urn");
            }
          });

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED =
      ConcurrentHashMap.newKeySet();

  // A cache of validated classes
  private static final Set<Class<? extends UnionTemplate>> UNION_VALIDATED =
      ConcurrentHashMap.newKeySet();

  private EntityValidator() {
    // Util class
  }

  /**
   * Validates an entity model defined in com.linkedin.metadata.entity.
   *
   * @param schema schema for the model
   */
  public static void validateEntitySchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema(
          "Entity '%s' must contain a non-optional 'urn' field of URN type", className);
    }

    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES)
        .forEach(
            field -> {
              ValidationUtils.invalidSchema(
                  "Entity '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
                  className, field.getName(), field.getType().getType());
            });

    ValidationUtils.nonOptionalFields(schema, NON_OPTIONAL_FIELDS)
        .forEach(
            field -> {
              ValidationUtils.invalidSchema(
                  "Entity '%s' must contain an optional '%s' field", className, field.getName());
            });
  }

  /**
   * Similar to {@link #validateEntitySchema(RecordDataSchema)} but take a {@link Class} instead and
   * caches results.
   */
  public static void validateEntitySchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateEntitySchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }

  /**
   * Similar to {@link #validateEntityUnionSchema(UnionDataSchema, String)} but take a {@link Class}
   * instead and caches results.
   */
  public static void validateEntityUnionSchema(@Nonnull Class<? extends UnionTemplate> clazz) {
    if (UNION_VALIDATED.contains(clazz)) {
      return;
    }

    validateEntityUnionSchema(ValidationUtils.getUnionSchema(clazz), clazz.getCanonicalName());
    UNION_VALIDATED.add(clazz);
  }

  /**
   * Validates the union of entity model defined in com.linkedin.metadata.entity.
   *
   * @param schema schema for the model
   */
  public static void validateEntityUnionSchema(
      @Nonnull UnionDataSchema schema, @Nonnull String entityClassName) {

    if (!ValidationUtils.isUnionWithOnlyComplexMembers(schema)) {
      ValidationUtils.invalidSchema(
          "Entity '%s' must be a union containing only record type members", entityClassName);
    }
  }

  /** Checks if an entity schema is valid. */
  public static boolean isValidEntitySchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (!VALIDATED.contains(clazz)) {
      try {
        validateEntitySchema(clazz);
      } catch (InvalidSchemaException ex) {
        return false;
      }
    }

    return true;
  }
}
