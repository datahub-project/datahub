package com.linkedin.metadata.models.annotation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;
import org.gradle.internal.impldep.com.google.common.collect.ImmutableMap;
import org.gradle.internal.impldep.com.google.common.collect.ImmutableSet;


/**
 * Simple object representation of the @Searchable annotation metadata.
 */
@Value
public class SearchableAnnotation {
  // Name of the field in the search index
  String fieldName;
  // Whether we should use the field for default autocomplete
  boolean isDefaultAutocomplete;
  // Whether or not to add field to filters. Note, if set to true, the first index setting needs to be of type KEYWORD
  boolean addToFilters;
  // List of settings for indexing the field. The first setting is used as default.
  List<IndexSetting> indexSettings;
  // (Optional) Weights to apply to score for a given value
  Map<Object, Double> weightsPerFieldValue;

  /**
   * Type of indexing to be done for the field
   * Each type maps to a different analyzer/normalizer
   */
  public enum IndexType {
    KEYWORD,
    KEYWORD_LOWERCASE,
    BOOLEAN,
    COUNT,
    BROWSE_PATH,
    TEXT,
    PATTERN,
    PARTIAL,
    PARTIAL_SHORT,
    PARTIAL_LONG,
    PARTIAL_PATTERN
  }

  public static final Map<IndexType, String> SUBFIELD_BY_TYPE =
      ImmutableMap.<IndexType, String>builder().put(IndexType.KEYWORD, "keyword")
          .put(IndexType.KEYWORD_LOWERCASE, "keyword")
          .put(IndexType.BOOLEAN, "boolean")
          .put(IndexType.TEXT, "delimited")
          .put(IndexType.PATTERN, "pattern")
          .put(IndexType.PARTIAL, "ngram")
          .put(IndexType.PARTIAL_SHORT, "ngram")
          .put(IndexType.PARTIAL_LONG, "ngram")
          .put(IndexType.PARTIAL_PATTERN, "pattern_ngram")
          .build();

  @Value
  public static class IndexSetting {
    // Type of index
    IndexType indexType;
    // (Optional) Whether the field is queryable without specifying the field in the query
    boolean addToDefaultQuery;
    // (Optional) Boost score for ranking
    Optional<Double> boostScore;
    // (Optional) Override fieldName (If set, creates a new search field with the specified name)
    Optional<String> overrideFieldName;
  }

  private static final Set<IndexType> FILTERABLE_INDEX_TYPES =
      ImmutableSet.of(IndexType.KEYWORD, IndexType.KEYWORD_LOWERCASE);

  private static IndexSetting getIndexSettingFromObject(@Nonnull final Object indexSettingObj) {
    if (!Map.class.isAssignableFrom(indexSettingObj.getClass())) {
      throw new IllegalArgumentException(
          "Failed to validate Searchable annotation object: Invalid value type provided for indexSettings (Expected Map)");
    }

    Map map = (Map) indexSettingObj;
    final Optional<String> indexType = AnnotationUtils.getField(map, "indexType", String.class);
    if (!indexType.isPresent() || !EnumUtils.isValidEnum(IndexType.class, indexType.get())) {
      throw new IllegalArgumentException(String.format(
          "Failed to validate required IndexSettings field 'indexType'. Invalid indexType provided. Valid types are %s",
          Arrays.toString(IndexType.values())));
    }

    final Optional<Boolean> addToDefaultQuery = AnnotationUtils.getField(map, "addToDefaultQuery", Boolean.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final Optional<String> overrideFieldName = AnnotationUtils.getField(map, "overrideFieldName", String.class);

    return new IndexSetting(IndexType.valueOf(indexType.get()), addToDefaultQuery.orElse(false), boostScore,
        overrideFieldName);
  }

  public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new IllegalArgumentException(
          "Failed to validate Searchable annotation object: Invalid value type provided (Expected Map)");
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    if (!fieldName.isPresent()) {
      throw new IllegalArgumentException(
          "Failed to validate required Searchable field 'fieldName' field of type String");
    }
    final Optional<Boolean> isDefaultAutocomplete =
        AnnotationUtils.getField(map, "isDefaultAutocomplete", Boolean.class);
    final Optional<Boolean> addToFilters = AnnotationUtils.getField(map, "addToDefaultQuery", Boolean.class);
    final Optional<List> indexSettingsList = AnnotationUtils.getField(map, "indexSettings", List.class);
    if (!indexSettingsList.isPresent() || indexSettingsList.get().isEmpty()) {
      throw new IllegalArgumentException(
          "Failed to validate required Searchable field 'indexSettings': field of type List cannot be empty");
    }
    final List<IndexSetting> indexSettings = new ArrayList<>(indexSettingsList.get().size());
    for (Object indexSettingObj : indexSettingsList.get()) {
      indexSettings.add(SearchableAnnotation.getIndexSettingFromObject(indexSettingObj));
    }

    // If the field is being added to filters, the first index setting must be of type KEYWORD
    if (addToFilters.orElse(false)) {
      if (indexSettings.get(0).getIndexType() != IndexType.KEYWORD) {
        throw new IllegalArgumentException("Failed to validate Searchable annotation: Fields with addToDefaultQuery "
            + "set to true, must have a setting of type KEYWORD as it's first index setting");
      }
    }

    final Optional<Map> weightsPerFieldValueMap =
        AnnotationUtils.getField(map, "overrideFieldName", Map.class).map(m -> (Map<Object, Double>) m);
    return new SearchableAnnotation(fieldName.get(), isDefaultAutocomplete.orElse(false), addToFilters.orElse(false),
        indexSettings, weightsPerFieldValueMap.orElse(ImmutableMap.of()));
  }
}
