package com.linkedin.metadata.models.annotation;

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
import org.gradle.internal.impldep.com.google.common.collect.ImmutableMap;
import org.gradle.internal.impldep.com.google.common.collect.ImmutableSet;


/**
 * Simple object representation of the @Searchable annotation metadata.
 */
@Value
public class SearchableAnnotation {

  public static final String ANNOTATION_NAME = "Searchable";

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
    KEYWORD_URN,
    BOOLEAN,
    COUNT,
    BROWSE_PATH,
    TEXT,
    PATTERN,
    URN,
    PARTIAL,
    PARTIAL_SHORT,
    PARTIAL_LONG,
    PARTIAL_PATTERN,
    PARTIAL_URN
  }

  public static final Map<IndexType, String> SUBFIELD_BY_TYPE =
      ImmutableMap.<IndexType, String>builder().put(IndexType.KEYWORD, "keyword")
          .put(IndexType.KEYWORD_LOWERCASE, "keyword")
          .put(IndexType.KEYWORD_URN, "keyword")
          .put(IndexType.BOOLEAN, "boolean")
          .put(IndexType.TEXT, "delimited")
          .put(IndexType.PATTERN, "pattern")
          .put(IndexType.URN, "urn_components")
          .put(IndexType.PARTIAL, "ngram")
          .put(IndexType.PARTIAL_SHORT, "ngram")
          .put(IndexType.PARTIAL_LONG, "ngram")
          .put(IndexType.PARTIAL_PATTERN, "pattern_ngram")
          .put(IndexType.PARTIAL_URN, "urn_components_ngram")
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
      ImmutableSet.of(IndexType.KEYWORD, IndexType.KEYWORD_LOWERCASE, IndexType.KEYWORD_URN);

  @Nonnull
  public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format("Failed to validate @%s annotation declared at %s: Invalid value type provided (Expected Map)",
              ANNOTATION_NAME, context));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    if (!fieldName.isPresent()) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid field 'fieldName'. Expected field of type String",
          ANNOTATION_NAME, context));
    }
    final Optional<Boolean> isDefaultAutocomplete =
        AnnotationUtils.getField(map, "isDefaultAutocomplete", Boolean.class);
    final Optional<Boolean> addToFilters = AnnotationUtils.getField(map, "addToFilters", Boolean.class);
    final Optional<List> indexSettingsList = AnnotationUtils.getField(map, "indexSettings", List.class);
    if (!indexSettingsList.isPresent() || indexSettingsList.get().isEmpty()) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid field 'indexSettings'. Expected field of type List to not be empty",
          ANNOTATION_NAME, context));
    }
    final List<IndexSetting> indexSettings = new ArrayList<>(indexSettingsList.get().size());
    for (Object indexSettingObj : indexSettingsList.get()) {
      indexSettings.add(SearchableAnnotation.getIndexSettingFromObject(indexSettingObj, context));
    }

    // If the field is being added to filters, the first index setting must be of type keyword
    if (addToFilters.orElse(false)) {
      if (!FILTERABLE_INDEX_TYPES.contains(indexSettings.get(0).getIndexType())) {
        throw new ModelValidationException(
            String.format("Failed to validate @%s annotation declared at %s: Fields with addToFilters ",
                ANNOTATION_NAME, context)
                + "set to true, must have a setting of type KEYWORD as it's first index setting. Current type: "
                + indexSettings.get(0).getIndexType());
      }
    }

    final Optional<Map> weightsPerFieldValueMap =
        AnnotationUtils.getField(map, "overrideFieldName", Map.class).map(m -> (Map<Object, Double>) m);
    return new SearchableAnnotation(fieldName.get(), isDefaultAutocomplete.orElse(false), addToFilters.orElse(false),
        indexSettings, weightsPerFieldValueMap.orElse(ImmutableMap.of()));
  }

  @Nonnull
  private static IndexSetting getIndexSettingFromObject(@Nonnull final Object indexSettingObj,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(indexSettingObj.getClass())) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid value type provided for indexSettings (Expected Map)",
          ANNOTATION_NAME, context));
    }

    Map map = (Map) indexSettingObj;
    final Optional<String> indexType = AnnotationUtils.getField(map, "indexType", String.class);
    if (!indexType.isPresent() || !EnumUtils.isValidEnum(IndexType.class, indexType.get())) {
      throw new ModelValidationException(String.format(
          "Failed to validate @%s annotation declared at %s: Invalid field 'indexType'. Invalid indexType provided. Valid types are %s",
          ANNOTATION_NAME, context, Arrays.toString(IndexType.values())));
    }

    final Optional<Boolean> addToDefaultQuery = AnnotationUtils.getField(map, "addToDefaultQuery", Boolean.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final Optional<String> overrideFieldName = AnnotationUtils.getField(map, "overrideFieldName", String.class);

    return new IndexSetting(IndexType.valueOf(indexType.get()), addToDefaultQuery.orElse(false), boostScore,
        overrideFieldName);
  }
}
