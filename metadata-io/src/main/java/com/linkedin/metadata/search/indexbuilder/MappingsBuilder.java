package com.linkedin.metadata.search.indexbuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexSetting;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MappingsBuilder {
  private static final Map<IndexType, String> SUBFIELD_BY_TYPE =
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

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(final EntitySpec entitySpec) {
    Map<String, Object> mappings = new HashMap<>();
    if (entitySpec.isBrowsable()) {
      mappings.put("browsePaths", getMappingsForBrowsePaths());
    }
    mappings.put("urn", getMappingsForUrn());
    entitySpec.getSearchableFieldSpecs()
        .forEach(searchableFieldSpec -> mappings.putAll(setMappingsForField(searchableFieldSpec)));
    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForBrowsePaths() {
    return ImmutableMap.<String, Object>builder().put("type", "text")
        .put("fields", ImmutableMap.of("length",
            ImmutableMap.<String, Object>builder().put("type", "token_count").put("analyzer", "slash_pattern").build()))
        .put("analyzer", "browse_path")
        .put("fielddata", true)
        .build();
  }

  private static Map<String, Object> getMappingsForUrn() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> setMappingsForField(final SearchableFieldSpec searchableFieldSpec) {
    Map<String, Object> mappingsForField = new HashMap<>();
    // Separate the settings with override and settings without
    Map<Boolean, List<IndexSetting>> indexSettingsHasOverride = searchableFieldSpec.getIndexSettings()
        .stream()
        .collect(Collectors.partitioningBy(setting -> setting.getOverrideFieldName().isPresent()));
    // Set the mappings for fields with overrides
    indexSettingsHasOverride.getOrDefault(true, ImmutableList.of())
        .forEach(setting -> mappingsForField.put(setting.getOverrideFieldName().get(), getMappingByType(setting)));

    List<IndexSetting> indexSettingsWithoutOverrides = indexSettingsHasOverride.getOrDefault(false, ImmutableList.of());
    if (indexSettingsWithoutOverrides.isEmpty()) {
      return mappingsForField;
    }

    // Use the first index setting without override as the default mapping
    ImmutableMap.Builder<String, Object> mapping = ImmutableMap.builder();
    mapping.putAll(getMappingByType(indexSettingsWithoutOverrides.get(0)));
    // If there are more settings, set as subField
    if (indexSettingsWithoutOverrides.size() > 1) {
      ImmutableMap.Builder<String, Object> subFields = ImmutableMap.builder();
      indexSettingsWithoutOverrides.stream()
          .skip(1)
          .forEach(setting -> subFields.put(SUBFIELD_BY_TYPE.getOrDefault(setting.getIndexType(), "default"),
              getMappingByType(setting)));
      mapping.put("fields", subFields.build());
    }
    mappingsForField.put(searchableFieldSpec.getFieldName(), mapping.build());
    return mappingsForField;
  }

  private static Map<String, Object> getMappingByType(IndexSetting indexSetting) {
    switch (indexSetting.getIndexType()) {
      case KEYWORD:
        return ImmutableMap.of("type", "keyword");
      case KEYWORD_LOWERCASE:
        return ImmutableMap.of("type", "keyword", "normalizer", "keyword_normalizer");
      case BOOLEAN:
        return ImmutableMap.of("type", "boolean");
      case COUNT:
        return ImmutableMap.of("type", "long");
      case TEXT:
        return ImmutableMap.of("type", "text", "analyzer", "word_delimited");
      case PATTERN:
        return ImmutableMap.of("type", "text", "analyzer", "pattern");
      case PARTIAL:
        return ImmutableMap.of("type", "text", "analyzer", "partial");
      case PARTIAL_SHORT:
        return ImmutableMap.of("type", "text", "analyzer", "partial_short");
      case PARTIAL_LONG:
        return ImmutableMap.of("type", "text", "analyzer", "partial_long");
      case PARTIAL_PATTERN:
        return ImmutableMap.of("type", "text", "analyzer", "partial_pattern");
      default:
        return ImmutableMap.of();
    }
  }
}
