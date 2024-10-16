package com.datahub.util.validator;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

/** Utility class to validate search document schemas. */
public final class DocumentValidator {

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

  private DocumentValidator() {
    // Util class
  }

  /**
   * Validates a search document model defined in com.linkedin.metadata.search.
   *
   * @param schema schema for the model
   */
  public static void validateDocumentSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema(
          "Document '%s' must contain an non-optional 'urn' field of URN type", className);
    }

    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES)
        .forEach(
            field -> {
              ValidationUtils.invalidSchema(
                  "Document '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
                  className, field.getName(), field.getType().getType());
            });

    ValidationUtils.nonOptionalFields(schema, NON_OPTIONAL_FIELDS)
        .forEach(
            field -> {
              ValidationUtils.invalidSchema(
                  "Document '%s' must contain an optional '%s' field", className, field.getName());
            });
  }

  /**
   * Similar to {@link #validateDocumentSchema(RecordDataSchema)} but take a {@link Class} instead
   * and caches results.
   */
  public static void validateDocumentSchema(@Nonnull Class<? extends RecordTemplate> clazz) {
    if (VALIDATED.contains(clazz)) {
      return;
    }

    validateDocumentSchema(ValidationUtils.getRecordSchema(clazz));
    VALIDATED.add(clazz);
  }
}
