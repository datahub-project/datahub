package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Validator for cross-annotation validation of SearchableAnnotation fields. Validates that fields
 * with the same searchLabel have compatible types.
 */
public class SearchableAnnotationValidator {

  // Compatible field type groups
  private static final Set<SearchableAnnotation.FieldType> TEXT_COMPATIBLE_TYPES =
      ImmutableSet.of(
          SearchableAnnotation.FieldType.KEYWORD,
          SearchableAnnotation.FieldType.TEXT,
          SearchableAnnotation.FieldType.TEXT_PARTIAL,
          SearchableAnnotation.FieldType.WORD_GRAM,
          SearchableAnnotation.FieldType.BROWSE_PATH,
          SearchableAnnotation.FieldType.URN,
          SearchableAnnotation.FieldType.URN_PARTIAL);

  private static final Set<SearchableAnnotation.FieldType> EXACT_MATCH_TYPES =
      ImmutableSet.of(
          SearchableAnnotation.FieldType.BOOLEAN,
          SearchableAnnotation.FieldType.COUNT,
          SearchableAnnotation.FieldType.DATETIME,
          SearchableAnnotation.FieldType.DOUBLE);

  /** Represents a field with its annotation and schema information */
  public static class AnnotatedField {
    private final SearchableAnnotation annotation;
    private final DataSchema.Type schemaType;
    private final String context;

    public AnnotatedField(
        SearchableAnnotation annotation, DataSchema.Type schemaType, String context) {
      this.annotation = annotation;
      this.schemaType = schemaType;
      this.context = context;
    }

    public SearchableAnnotation getAnnotation() {
      return annotation;
    }

    public DataSchema.Type getSchemaType() {
      return schemaType;
    }

    public String getContext() {
      return context;
    }
  }

  /** Validates that all fields with the same searchLabel have compatible types */
  public static void validateCrossAnnotationCompatibility(
      @Nonnull List<AnnotatedField> annotatedFields) {
    Map<String, List<AnnotatedField>> searchLabelGroups = new HashMap<>();

    // Group fields by searchLabel
    for (AnnotatedField field : annotatedFields) {
      SearchableAnnotation annotation = field.getAnnotation();

      if (annotation.getSearchLabel().isPresent()) {
        String searchLabel = annotation.getSearchLabel().get();
        searchLabelGroups.computeIfAbsent(searchLabel, k -> new ArrayList<>()).add(field);
      }
    }

    // Validate searchLabel groups
    for (Map.Entry<String, List<AnnotatedField>> entry : searchLabelGroups.entrySet()) {
      validateFieldGroup(entry.getKey(), entry.getValue(), "searchLabel");
    }
  }

  private static void validateFieldGroup(
      String labelValue, List<AnnotatedField> fields, String labelType) {
    if (fields.size() <= 1) {
      return; // No validation needed for single field
    }

    AnnotatedField firstField = fields.get(0);
    DataSchema.Type firstSchemaType = firstField.getSchemaType();
    SearchableAnnotation.FieldType firstFieldType = firstField.getAnnotation().getFieldType();

    for (int i = 1; i < fields.size(); i++) {
      AnnotatedField currentField = fields.get(i);
      DataSchema.Type currentSchemaType = currentField.getSchemaType();
      SearchableAnnotation.FieldType currentFieldType = currentField.getAnnotation().getFieldType();

      // Check PDL type compatibility
      if (!firstSchemaType.equals(currentSchemaType)) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: fields with the same %s '%s' must have the same PDL type. "
                    + "Field at %s has type %s, but field at %s has type %s",
                SearchableAnnotation.ANNOTATION_NAME,
                labelType,
                labelValue,
                firstField.getContext(),
                firstSchemaType,
                currentField.getContext(),
                currentSchemaType));
      }

      // Check fieldType compatibility
      if (!areFieldTypesCompatible(firstFieldType, currentFieldType)) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: fields with the same %s '%s' must have compatible fieldTypes. "
                    + "Field at %s has fieldType %s, but field at %s has fieldType %s",
                SearchableAnnotation.ANNOTATION_NAME,
                labelType,
                labelValue,
                firstField.getContext(),
                firstFieldType,
                currentField.getContext(),
                currentFieldType));
      }
    }
  }

  private static boolean areFieldTypesCompatible(
      SearchableAnnotation.FieldType type1, SearchableAnnotation.FieldType type2) {
    // Same type is always compatible
    if (type1.equals(type2)) {
      return true;
    }

    // Check if both are in text-compatible group
    if (TEXT_COMPATIBLE_TYPES.contains(type1) && TEXT_COMPATIBLE_TYPES.contains(type2)) {
      return true;
    }

    // Exact match types must be identical (already checked above)
    if (EXACT_MATCH_TYPES.contains(type1) || EXACT_MATCH_TYPES.contains(type2)) {
      return false;
    }

    // OBJECT type is not compatible with anything else
    if (type1 == SearchableAnnotation.FieldType.OBJECT
        || type2 == SearchableAnnotation.FieldType.OBJECT) {
      return false;
    }

    // Default to incompatible for any other combinations
    return false;
  }
}
