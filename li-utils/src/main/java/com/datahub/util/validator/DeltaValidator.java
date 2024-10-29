package com.datahub.util.validator;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/** Utility class to validate delta event schemas. */
public final class DeltaValidator {

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED =
      ConcurrentHashMap.newKeySet();

  private DeltaValidator() {
    // Util class
  }

  /**
   * Validates a delta model defined in com.linkedin.metadata.delta.
   *
   * @param schema schema for the model
   */
  public static void validateDeltaSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema(
          "Delta '%s' must contain an non-optional 'urn' field of URN type", className);
    }

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, DeltaValidator::isValidDeltaField)) {
      ValidationUtils.invalidSchema(
          "Delta '%s' must contain an non-optional 'delta' field of UNION type", className);
    }
  }

  /**
   * Similar to {@link #validateDeltaSchema(RecordDataSchema)} but take a {@link Class} instead and
   * caches results.
   */
  public static void validateDeltaSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateDeltaSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }

  private static boolean isValidDeltaField(@Nonnull RecordDataSchema.Field field) {
    return field.getName().equals("delta")
        && !field.getOptional()
        && field.getType().getType() == DataSchema.Type.UNION;
  }
}
