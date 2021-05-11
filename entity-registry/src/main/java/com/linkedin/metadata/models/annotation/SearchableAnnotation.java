package com.linkedin.metadata.models.annotation;

import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;


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

    /**
     * Type of indexing to be done for the field
     * Each type maps to a different analyzer/normalizer
     */
    public enum IndexType {
        KEYWORD,
        BOOLEAN,
        BROWSE_PATH,
        DELIMITED,
        PATTERN,
        PARTIAL,
        PARTIAL_SHORT,
        PARTIAL_LONG,
        PARTIAL_PATTERN
    }

    @Value
    public static class IndexSetting {
        // Type of index
        IndexType indexType;
        // (Optional) Whether the field is queryable without specifying the field in the query
        boolean addToDefaultQuery;
        // (Optional) Boost score for ranking
        double boostScore;
    }

    private static IndexSetting getIndexSettingFromObject(@Nonnull final Object indexSettingObj) {
        if (!Map.class.isAssignableFrom(indexSettingObj.getClass())) {
            throw new IllegalArgumentException("Failed to validate Searchable annotation object: Invalid value type provided for indexSettings (Expected Map)");
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

        return new IndexSetting(IndexType.valueOf(indexType.get()), addToDefaultQuery.orElse(false), boostScore.orElse(1.0));
    }

    public static SearchableAnnotation fromPegasusAnnotationObject(@Nonnull final Object annotationObj) {
        if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
            throw new IllegalArgumentException("Failed to validate Searchable annotation object: Invalid value type provided (Expected Map)");
        }

        Map map = (Map) annotationObj;
        final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
        if (!fieldName.isPresent()) {
            throw new IllegalArgumentException("Failed to validate required Searchable field 'fieldName' field of type String");
        }
        final Optional<Boolean> isDefaultAutocomplete = AnnotationUtils.getField(map, "isDefaultAutocomplete", Boolean.class);
        final Optional<Boolean> addToFilters = AnnotationUtils.getField(map, "addToDefaultQuery", Boolean.class);
        final Optional<List> indexSettingsList = AnnotationUtils.getField(map, "indexSettings", List.class);
        if (!indexSettingsList.isPresent() || indexSettingsList.get().isEmpty()) {
            throw new IllegalArgumentException("Failed to validate required Searchable field 'indexSettings': field of type List cannot be empty");
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
        return new SearchableAnnotation(fieldName.get(), isDefaultAutocomplete.orElse(false), addToFilters.orElse(false), indexSettings);
    }
}
