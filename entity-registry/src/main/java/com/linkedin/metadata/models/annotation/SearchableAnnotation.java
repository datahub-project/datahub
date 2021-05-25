package com.linkedin.metadata.models.annotation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;
import org.gradle.internal.impldep.com.google.common.collect.ImmutableMap;


/**
 * Simple object representation of the @Searchable annotation metadata.
 */
@Value
public class SearchableAnnotation {

  public static final String ANNOTATION_NAME = "Searchable";

  // Name of the field in the search index. Defaults to the field name in the schema
  String fieldName;
  // Type of the field. Defines how the field is indexed and matched
  FieldType fieldType;
  // Whether we should match the field for the default search query
  boolean queryByDefault;
  // Whether we should use the field for default autocomplete
  boolean enableAutocomplete;
  // Whether or not to add field to filters.
  boolean addToFilters;
  // Boost multiplier to the match score. Matches on fields with higher boost score ranks higher
  double boostScore;
  // If set, add a index field of the given name that checks whether the field exists
  Optional<String> hasValuesFieldName;
  // If set, add a index field of the given name that checks the number of elements
  Optional<String> numValuesFieldName;
  // (Optional) Weights to apply to score for a given value
  Map<Object, Double> weightsPerFieldValue;

  public enum FieldType {
    KEYWORD, TEXT, TEXT_WITH_PARTIAL_MATCHING, BROWSE_PATH, URN, URN_WITH_PARTIAL_MATCHING, BOOLEAN, COUNT
  }

  @Nonnull
  public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName, @Nonnull final DataSchema.Type schemaDataType,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format("Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    final Optional<String> fieldType = AnnotationUtils.getField(map, "fieldType", String.class);
    if (!fieldType.isPresent() || !EnumUtils.isValidEnum(FieldType.class, fieldType.get())) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid field 'fieldType'. Invalid fieldType provided. Valid types are %s",
          ANNOTATION_NAME, context, Arrays.toString(FieldType.values())));
    }

    final Optional<Boolean> queryByDefault = AnnotationUtils.getField(map, "queryByDefault", Boolean.class);
    final Optional<Boolean> enableAutocomplete = AnnotationUtils.getField(map, "enableAutocomplete", Boolean.class);
    final Optional<Boolean> addToFilters = AnnotationUtils.getField(map, "addToFilters", Boolean.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final Optional<String> hasValuesFieldName = AnnotationUtils.getField(map, "hasValuesFieldName", String.class);
    final Optional<String> numValuesFieldName = AnnotationUtils.getField(map, "numValuesFieldName", String.class);

    final Optional<Map> weightsPerFieldValueMap =
        AnnotationUtils.getField(map, "weightsPerFieldValue", Map.class).map(m -> (Map<Object, Double>) m);
    return new SearchableAnnotation(fieldName.orElse(schemaFieldName), FieldType.valueOf(fieldType.get()),
        queryByDefault.orElse(false), enableAutocomplete.orElse(false), addToFilters.orElse(false),
        boostScore.orElse(1.0), hasValuesFieldName, numValuesFieldName,
        weightsPerFieldValueMap.orElse(ImmutableMap.of()));
  }
}
