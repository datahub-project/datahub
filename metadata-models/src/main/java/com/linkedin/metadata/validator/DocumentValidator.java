package com.linkedin.metadata.validator;

import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;


public class DocumentValidator {

  // Allowed non-optional fields. All other fields must be optional.
  private static final Set<String> NON_OPTIONAL_FIELDS = Collections.unmodifiableSet(new HashSet<String>() {
    {
      add("urn");
    }
  });

  // A cache of validated classes
  private static final Set<Class<? extends RecordTemplate>> VALIDATED = ConcurrentHashMap.newKeySet();

  private DocumentValidator() {
    // Util class
  }

  /**
   * Validates a search document model defined in com.linkedin.metadata.search.
   *
   * @param schema schema for the model
   */
  public static void validateSchema(@Nonnull RecordDataSchema schema) {

    final String className = schema.getBindingName();

    if (!ValidationUtils.schemaHasExactlyOneSuchField(schema, ValidationUtils::isValidUrnField)) {
      ValidationUtils.invalidSchema("Document '%s' must contain an non-optional 'urn' field of URN type", className);
    }

    ValidationUtils.fieldsUsingInvalidType(schema, ValidationUtils.PRIMITIVE_TYPES).forEach(field -> {
      if (field.getType().getType() == DataSchema.Type.ARRAY) {
        validateArrayType(field, className);
      } else {
        ValidationUtils.invalidSchema("Document '%s' contains a field '%s' that makes use of a disallowed type '%s'.",
            className, field.getName(), field.getType().getType());
      }
    });

    ValidationUtils.nonOptionalFields(schema, NON_OPTIONAL_FIELDS).forEach(field -> {
      ValidationUtils.invalidSchema("Document '%s' must contain an optional '%s' field", className, field.getName());
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

  private static void validateArrayType(@Nonnull RecordDataSchema.Field field, String className) {
    DataSchema.Type type = ((ArrayDataSchema) field.getType()).getItems().getType();

    if (!ValidationUtils.PRIMITIVE_TYPES.contains(type)) {
      ValidationUtils.invalidSchema("Document '%s' contains an array field '%s' that uses a disallowed type '%s'.",
          className, field.getName(), type);
    }
  }
}