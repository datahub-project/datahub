package com.linkedin.metadata.validator;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;


public class EntityValidator {

  // Allowed non-optional fields. All other fields must be optional.
  private static final Set<String> NON_OPTIONAL_FIELDS = Collections.unmodifiableSet(new HashSet<String>() {
    {
      add("urn");
    }
  });

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED = ConcurrentHashMap.newKeySet();

  private EntityValidator() {
    // Util class
  }

  /**
   * Validates an entity model defined in com.linkedin.metadata.entity.
   *
   * @param schema schema for the model
   */
  public static void validateSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema("Entity '%s' must contain a non-optional 'urn' field of URN type", className);
    }

    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES).forEach(field -> {
      ValidationUtils.invalidSchema("Entity '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
          className, field.getName(), field.getType().getType());
    });

    ValidationUtils.nonOptionalFields(schema, NON_OPTIONAL_FIELDS).forEach(field -> {
      ValidationUtils.invalidSchema("Entity '%s' must contain an optional '%s' field", className, field.getName());
    });
  }

  /**
   * Similar to {@link #validateSchema(RecordDataSchema)} but take a {@link Class} instead and caches results.
   */
  public static void validateSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }
}