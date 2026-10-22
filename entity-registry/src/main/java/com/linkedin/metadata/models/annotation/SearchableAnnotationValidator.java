package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.ModelValidationException;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Validator for cross-annotation validation of SearchableAnnotation fields. Fields that share an
 * indexed name (a search label at one degree of separation) share one indexed field, so they must
 * have compatible types, the same resolved boost, and the same hasValuesFieldName.
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

  /**
   * Fields that share an indexed name must agree on PDL type, field type, resolved boost, resolved
   * {@code queryByDefault}, and hasValuesFieldName. Search V3 also requires one root and one
   * subfield set for each indexed name and entity field. A farther degree of one label must score
   * lower than a nearer one. {@code queryByDefault} may differ across degrees. One aspect location
   * has one degree.
   */
  public static void validateSharedLabels(@Nonnull Collection<EntitySpec> entitySpecs) {
    final List<AnnotatedField> fields = new ArrayList<>();
    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          fields.add(
              new AnnotatedField(
                  fieldSpec.getSearchableAnnotation(),
                  fieldSpec.getPegasusSchema().getDereferencedType(),
                  entitySpec.getName()
                      + "."
                      + aspectSpec.getName()
                      + "."
                      + fieldSpec.getSearchableAnnotation().getFieldName()));
        }
      }
    }
    validateCrossAnnotationCompatibility(fields);
    validateDegreeByLocation(entitySpecs);
  }

  /**
   * Validates that all fields with the same indexed name have compatible types, one weight, one
   * queryByDefault, and one hasValuesFieldName, and that a label's score falls as the degree rises.
   */
  public static void validateCrossAnnotationCompatibility(
      @Nonnull List<AnnotatedField> annotatedFields) {
    Map<String, List<AnnotatedField>> indexedNameGroups = new HashMap<>();
    Map<String, List<AnnotatedField>> labelGroups = new HashMap<>();
    for (AnnotatedField field : annotatedFields) {
      SearchableAnnotation annotation = field.getAnnotation();
      if (!annotation.isV3Searchable()) {
        continue;
      }
      SearchableAnnotation resolved = annotation.resolvedForV3();
      AnnotatedField resolvedField =
          new AnnotatedField(resolved, field.getSchemaType(), field.getContext());
      resolved
          .indexedFieldName()
          .ifPresent(
              indexedName ->
                  indexedNameGroups
                      .computeIfAbsent(indexedName, key -> new ArrayList<>())
                      .add(resolvedField));
      resolved
          .getSearchLabel()
          .filter(label -> !label.isEmpty())
          .ifPresent(
              label ->
                  labelGroups.computeIfAbsent(label, key -> new ArrayList<>()).add(resolvedField));
    }
    for (Map.Entry<String, List<AnnotatedField>> entry : indexedNameGroups.entrySet()) {
      validateFieldGroup(entry.getKey(), entry.getValue(), "indexed field");
      validateHasValuesFieldName(entry.getKey(), entry.getValue());
      validateV3Label(entry.getKey(), entry.getValue());
    }
    labelGroups.forEach(SearchableAnnotationValidator::validateNonIncreasingBoost);
    validateV3Mappings(annotatedFields);
    validateV3Destinations(annotatedFields, SearchableAnnotation::getEntityFieldName);
  }

  /**
   * The same aspect location uses one degree on every entity. The location is the aspect name plus
   * the schema path of the annotation in effect.
   */
  private static void validateDegreeByLocation(@Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, AnnotatedField> degreeByLocation = new HashMap<>();
    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          SearchableAnnotation resolved = fieldSpec.getSearchableAnnotation().resolvedForV3();
          if (!resolved.isV3Searchable() || resolved.getSearchLabel().isEmpty()) {
            continue;
          }
          String location = aspectSpec.getName() + fieldSpec.getPath().toString();
          AnnotatedField current =
              new AnnotatedField(
                  resolved,
                  fieldSpec.getPegasusSchema().getDereferencedType(),
                  entitySpec.getName() + "." + location);
          AnnotatedField previous = degreeByLocation.putIfAbsent(location, current);
          if (previous != null && previous.getAnnotation().getDegree() != resolved.getDegree()) {
            throw new ModelValidationException(
                String.format(
                    "Failed to validate @%s annotations: aspect location '%s' must have one degree."
                        + " Field at %s has degree %s, but field at %s has degree %s",
                    SearchableAnnotation.ANNOTATION_NAME,
                    location,
                    previous.getContext(),
                    previous.getAnnotation().getDegree(),
                    current.getContext(),
                    resolved.getDegree()));
          }
        }
      }
    }
  }

  /** A farther degree of one label must have a lower schema score than a nearer degree. */
  private static void validateNonIncreasingBoost(
      @Nonnull String label, @Nonnull List<AnnotatedField> fields) {
    Map<Integer, AnnotatedField> byDegree = new HashMap<>();
    for (AnnotatedField field : fields) {
      int degree = field.getAnnotation().getDegree();
      AnnotatedField previous = byDegree.putIfAbsent(degree, field);
      if (previous != null
          && Double.compare(
                  previous.getAnnotation().getBoostScore(), field.getAnnotation().getBoostScore())
              != 0) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: fields with indexed field '%s' must have the"
                    + " same boostScore. Field at %s has boostScore %s, but field at %s has"
                    + " boostScore %s",
                SearchableAnnotation.ANNOTATION_NAME,
                SearchableAnnotation.indexedFieldName(label, degree),
                previous.getContext(),
                previous.getAnnotation().getBoostScore(),
                field.getContext(),
                field.getAnnotation().getBoostScore()));
      }
    }
    AnnotatedField nearer = null;
    int furthest = byDegree.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
    for (int degree = 0; degree <= furthest; degree++) {
      AnnotatedField current = byDegree.get(degree);
      if (current == null) {
        continue;
      }
      if (nearer != null
          && current.getAnnotation().getBoostScore() >= nearer.getAnnotation().getBoostScore()) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: boostScore of label '%s' must decrease with"
                    + " degree. Degree %s at %s has boostScore %s, but degree %s at %s has"
                    + " boostScore %s",
                SearchableAnnotation.ANNOTATION_NAME,
                label,
                nearer.getAnnotation().getDegree(),
                nearer.getContext(),
                nearer.getAnnotation().getBoostScore(),
                current.getAnnotation().getDegree(),
                current.getContext(),
                current.getAnnotation().getBoostScore()));
      }
      nearer = current;
    }
  }

  /**
   * An indexed field publishes one hasValues boolean. Every source declares that name, or none do.
   * Search V3 indexes degree 0 as the declared name and degree n as {@code <name>_<n>}. V2 keeps
   * the declared name. A source that omits it, or names a different boolean, would make a false
   * filter miss some of the field.
   */
  private static void validateHasValuesFieldName(
      @Nonnull String label, @Nonnull List<AnnotatedField> fields) {
    if (fields.size() <= 1) {
      return;
    }
    AnnotatedField firstField = fields.get(0);
    Optional<String> firstName = firstField.getAnnotation().getHasValuesFieldName();
    for (int i = 1; i < fields.size(); i++) {
      AnnotatedField currentField = fields.get(i);
      Optional<String> currentName = currentField.getAnnotation().getHasValuesFieldName();
      if (firstName.equals(currentName)) {
        continue;
      }
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotations: fields with the same indexed field '%s' must"
                  + " declare the same hasValuesFieldName. Field at %s has %s, but field at %s has"
                  + " %s",
              SearchableAnnotation.ANNOTATION_NAME,
              label,
              firstField.getContext(),
              firstName.orElse("none"),
              currentField.getContext(),
              currentName.orElse("none")));
    }
  }

  /** Subfields need a label, and each mapping must be able to parse the field's PDL type. */
  private static void validateV3Mappings(@Nonnull List<AnnotatedField> annotatedFields) {
    for (AnnotatedField field : annotatedFields) {
      SearchableAnnotation annotation = field.getAnnotation();
      if (!annotation.isV3Searchable()) {
        continue;
      }
      SearchableAnnotation resolved = annotation.resolvedForV3();
      boolean labeled = hasDestination(resolved.getSearchLabel(), resolved.getEntityFieldName());
      for (FieldMapping mapping : resolved.getV3Mappings()) {
        if (!mapping.isRoot() && !labeled) {
          throw new ModelValidationException(
              String.format(
                  "Failed to validate @%s annotation declared at %s: a mappings subfield requires a"
                      + " searchLabel",
                  SearchableAnnotation.ANNOTATION_NAME, field.getContext()));
        }
        // KEYWORD is the string fallback for field types such as TEXT, including longs that V2
        // already indexes as text. Other mapping types must be able to parse the PDL value.
        if (mapping.getType() != FieldMapping.Type.KEYWORD
            && !mapping.accepts(field.getSchemaType())) {
          throw new ModelValidationException(
              String.format(
                  "Failed to validate @%s annotation declared at %s: mapping type %s cannot index"
                      + " PDL type %s",
                  SearchableAnnotation.ANNOTATION_NAME,
                  field.getContext(),
                  mapping.getType(),
                  field.getSchemaType()));
        }
      }
    }
  }

  /** Fields that copy into the same entity field must resolve to one root and one subfield set. */
  private static void validateV3Destinations(
      @Nonnull List<AnnotatedField> annotatedFields,
      @Nonnull java.util.function.Function<SearchableAnnotation, Optional<String>> destination) {
    Map<String, List<AnnotatedField>> groups = new HashMap<>();
    for (AnnotatedField field : annotatedFields) {
      if (!field.getAnnotation().isV3Searchable()) {
        continue;
      }
      SearchableAnnotation resolved = field.getAnnotation().resolvedForV3();
      destination
          .apply(resolved)
          .filter(name -> !name.isEmpty())
          .ifPresent(
              name ->
                  groups
                      .computeIfAbsent(name, key -> new ArrayList<>())
                      .add(
                          new AnnotatedField(resolved, field.getSchemaType(), field.getContext())));
    }
    for (Map.Entry<String, List<AnnotatedField>> entry : groups.entrySet()) {
      if (entry.getValue().size() > 1) {
        validateV3Label(entry.getKey(), entry.getValue());
      }
    }
  }

  private static void validateV3Label(@Nonnull String label, @Nonnull List<AnnotatedField> fields) {
    List<List<FieldMapping>> contributors = new ArrayList<>();
    for (AnnotatedField field : fields) {
      SearchableAnnotation resolved = field.getAnnotation().resolvedForV3();
      FieldMapping root = FieldMapping.rootOf(resolved.getV3Mappings());
      if (root.getType() == FieldMapping.Type.OBJECT) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchLabel '%s' cannot be an"
                    + " object",
                SearchableAnnotation.ANNOTATION_NAME, field.getContext(), label));
      }
      contributors.add(resolved.getV3Mappings());
    }
    FieldMapping.union(label, contributors);
  }

  private static boolean hasDestination(
      Optional<String> searchLabel, Optional<String> entityFieldName) {
    return searchLabel.filter(label -> !label.isEmpty()).isPresent()
        || entityFieldName.filter(name -> !name.isEmpty()).isPresent();
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

      // One indexed field has one query weight
      if (Double.compare(
              firstField.getAnnotation().getBoostScore(),
              currentField.getAnnotation().getBoostScore())
          != 0) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: fields with the same %s '%s' must have the same boostScore. "
                    + "Field at %s has boostScore %s, but field at %s has boostScore %s",
                SearchableAnnotation.ANNOTATION_NAME,
                labelType,
                labelValue,
                firstField.getContext(),
                firstField.getAnnotation().getBoostScore(),
                currentField.getContext(),
                currentField.getAnnotation().getBoostScore()));
      }

      // One indexed field has one default-query flag. Degrees of one label may differ.
      if (firstField.getAnnotation().isQueryByDefault()
          != currentField.getAnnotation().isQueryByDefault()) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotations: fields with the same %s '%s' must have the same"
                    + " queryByDefault. Field at %s has queryByDefault %s, but field at %s has"
                    + " queryByDefault %s",
                SearchableAnnotation.ANNOTATION_NAME,
                labelType,
                labelValue,
                firstField.getContext(),
                firstField.getAnnotation().isQueryByDefault(),
                currentField.getContext(),
                currentField.getAnnotation().isQueryByDefault()));
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
