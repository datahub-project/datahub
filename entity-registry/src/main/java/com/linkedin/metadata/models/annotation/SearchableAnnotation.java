package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

/** Simple object representation of the @Searchable annotation metadata. */
@Value
public class SearchableAnnotation {

  public static final String FIELD_NAME_ALIASES = "fieldNameAliases";
  public static final String ANNOTATION_NAME = "Searchable";
  public static final Set<FieldType> OBJECT_FIELD_TYPES =
      ImmutableSet.of(FieldType.OBJECT, FieldType.MAP_ARRAY);
  private static final Set<FieldType> DEFAULT_QUERY_FIELD_TYPES =
      ImmutableSet.of(
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

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
  // Whether or not to add the "has values" to filters.
  boolean addHasValuesToFilters;
  // Display name of the filter
  Optional<String> filterNameOverride;
  // Display name of the has values filter
  Optional<String> hasValuesFilterNameOverride;
  // Boost multiplier to the match score. Matches on fields with higher boost score ranks higher
  double boostScore;
  // If set, add a index field of the given name that checks whether the field exists
  Optional<String> hasValuesFieldName;
  // If set, add a index field of the given name that checks the number of elements
  Optional<String> numValuesFieldName;
  // (Optional) Weights to apply to score for a given value
  Map<Object, Double> weightsPerFieldValue;
  // (Optional) Aliases for this given field that can be used for sorting etc.
  List<String> fieldNameAliases;
  // Whether to create a missing field aggregation when querying the corresponding field,
  // only adds to query time not mapping
  boolean includeQueryEmptyAggregation;

  boolean includeSystemModifiedAt;

  Optional<String> systemModifiedAtFieldName;

  public enum FieldType {
    KEYWORD,
    TEXT,
    TEXT_PARTIAL,
    BROWSE_PATH,
    URN,
    URN_PARTIAL,
    BOOLEAN,
    COUNT,
    DATETIME,
    OBJECT,
    BROWSE_PATH_V2,
    WORD_GRAM,
    DOUBLE,
    MAP_ARRAY
  }

  @Nonnull
  public static SearchableAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final DataSchema.Type schemaDataType,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    final Optional<String> fieldType = AnnotationUtils.getField(map, "fieldType", String.class);
    if (fieldType.isPresent() && !EnumUtils.isValidEnum(FieldType.class, fieldType.get())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field 'fieldType'. Invalid fieldType provided. Valid types are %s",
              ANNOTATION_NAME, context, Arrays.toString(FieldType.values())));
    }

    final Optional<Boolean> queryByDefault =
        AnnotationUtils.getField(map, "queryByDefault", Boolean.class);
    final Optional<Boolean> enableAutocomplete =
        AnnotationUtils.getField(map, "enableAutocomplete", Boolean.class);
    final Optional<Boolean> addToFilters =
        AnnotationUtils.getField(map, "addToFilters", Boolean.class);
    final Optional<Boolean> addHasValuesToFilters =
        AnnotationUtils.getField(map, "addHasValuesToFilters", Boolean.class);
    final Optional<String> filterNameOverride =
        AnnotationUtils.getField(map, "filterNameOverride", String.class);
    final Optional<String> hasValuesFilterNameOverride =
        AnnotationUtils.getField(map, "hasValuesFilterNameOverride", String.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final Optional<String> hasValuesFieldName =
        AnnotationUtils.getField(map, "hasValuesFieldName", String.class);
    final Optional<String> numValuesFieldName =
        AnnotationUtils.getField(map, "numValuesFieldName", String.class);
    final Optional<Map> weightsPerFieldValueMap =
        AnnotationUtils.getField(map, "weightsPerFieldValue", Map.class)
            .map(m -> (Map<Object, Double>) m);
    final Optional<Boolean> includeQueryEmptyAggregation =
        AnnotationUtils.getField(map, "includeQueryEmptyAggregation", Boolean.class);
    final List<String> fieldNameAliases = getFieldNameAliases(map);

    final FieldType resolvedFieldType = getFieldType(fieldType, schemaDataType);
    final Optional<Boolean> includeSystemModifiedAt =
        AnnotationUtils.getField(map, "includeSystemModifiedAt", Boolean.class);
    final Optional<String> systemModifiedAtFieldName =
        AnnotationUtils.getField(map, "systemModifiedAtFieldName", String.class);
    return new SearchableAnnotation(
        fieldName.orElse(schemaFieldName),
        resolvedFieldType,
        getQueryByDefault(queryByDefault, resolvedFieldType),
        enableAutocomplete.orElse(false),
        addToFilters.orElse(false),
        addHasValuesToFilters.orElse(false),
        filterNameOverride,
        hasValuesFilterNameOverride,
        boostScore.orElse(1.0),
        hasValuesFieldName,
        numValuesFieldName,
        weightsPerFieldValueMap.orElse(ImmutableMap.of()),
        fieldNameAliases,
        includeQueryEmptyAggregation.orElse(false),
        includeSystemModifiedAt.orElse(false),
        systemModifiedAtFieldName);
  }

  private static FieldType getFieldType(
      Optional<String> maybeFieldType, DataSchema.Type schemaDataType) {
    if (!maybeFieldType.isPresent()) {
      return getDefaultFieldType(schemaDataType);
    }
    return FieldType.valueOf(maybeFieldType.get());
  }

  private static FieldType getDefaultFieldType(DataSchema.Type schemaDataType) {
    switch (schemaDataType) {
      case INT:
        return FieldType.COUNT;
      case MAP:
        return FieldType.KEYWORD;
      case FLOAT:
      case DOUBLE:
        return FieldType.DOUBLE;
      default:
        return FieldType.TEXT;
    }
  }

  private static Boolean getQueryByDefault(
      Optional<Boolean> maybeQueryByDefault, FieldType fieldType) {
    if (!maybeQueryByDefault.isPresent()) {
      if (DEFAULT_QUERY_FIELD_TYPES.contains(fieldType)) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    return maybeQueryByDefault.get();
  }

  public String getFilterName() {
    return filterNameOverride.orElse(fieldName);
  }

  public String getHasValuesFilterName() {
    return hasValuesFilterNameOverride.orElse(
        hasValuesFieldName.orElse(String.format("has%s", capitalizeFirstLetter(fieldName))));
  }

  private static String capitalizeFirstLetter(String str) {
    if (str == null || str.length() == 0) {
      return str;
    } else {
      return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
  }

  private static List<String> getFieldNameAliases(Map map) {
    final List<String> aliases = new ArrayList<>();
    final Optional<List> fieldNameAliases =
        AnnotationUtils.getField(map, FIELD_NAME_ALIASES, List.class);
    if (fieldNameAliases.isPresent()) {
      for (Object alias : fieldNameAliases.get()) {
        aliases.add((String) alias);
      }
    }
    return aliases;
  }
}
