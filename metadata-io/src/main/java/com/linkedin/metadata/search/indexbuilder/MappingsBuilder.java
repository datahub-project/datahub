package com.linkedin.metadata.search.indexbuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexSetting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MappingsBuilder {

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final EntitySpec entitySpec) {
    Map<String, Object> mappings = new HashMap<>();
    mappings.put("urn", getMappingsForUrn());
    entitySpec.getSearchableFieldSpecs()
        .forEach(searchableFieldSpec -> mappings.putAll(setMappingsForField(searchableFieldSpec)));
    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForUrn() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> setMappingsForField(@Nonnull final SearchableFieldSpec searchableFieldSpec) {
    Map<String, Object> mappingsForField = new HashMap<>();
    // Separate the settings with override and settings without
    Map<Boolean, List<IndexSetting>> indexSettingsHasOverride = searchableFieldSpec.getIndexSettings()
        .stream()
        .collect(Collectors.partitioningBy(setting -> setting.getOverrideFieldName().isPresent()));
    // Set the mappings for fields with overrides
    indexSettingsHasOverride.getOrDefault(true, ImmutableList.of())
        .forEach(setting -> getMappingByType(setting).ifPresent(
            mappings -> mappingsForField.put(setting.getOverrideFieldName().get(), mappings)));

    List<IndexSetting> indexSettingsWithoutOverrides = indexSettingsHasOverride.getOrDefault(false, ImmutableList.of());
    if (indexSettingsWithoutOverrides.isEmpty()) {
      return mappingsForField;
    }

    // Use the first index setting without override as the default mapping
    Map<String, Object> mapping = new HashMap<>();
    getMappingByType(indexSettingsWithoutOverrides.get(0)).ifPresent(mapping::putAll);
    // If there are more settings, set as subField
    if (indexSettingsWithoutOverrides.size() > 1) {
      ImmutableMap.Builder<String, Object> subFields = ImmutableMap.builder();
      indexSettingsWithoutOverrides.stream()
          .skip(1)
          .forEach(setting -> getMappingByType(setting).ifPresent(mappings -> subFields.put(
              SearchableAnnotation.SUBFIELD_BY_TYPE.getOrDefault(setting.getIndexType(), "default"), mappings)));
      mapping.put("fields", subFields.build());
    }
    if (!mapping.isEmpty()) {
      mappingsForField.put(searchableFieldSpec.getFieldName(), mapping);
    }
    return mappingsForField;
  }

  private static Optional<Map<String, Object>> getMappingByType(IndexSetting indexSetting) {
    Map<String, Object> mappings = null;
    switch (indexSetting.getIndexType()) {
      case KEYWORD:
        mappings = ImmutableMap.of("type", "keyword");
        break;
      case KEYWORD_LOWERCASE:
        mappings = ImmutableMap.of("type", "keyword", "normalizer", "keyword_normalizer");
        break;
      case KEYWORD_URN:
        mappings = ImmutableMap.of("type", "text", "analyzer", "urn_component", "fielddata", true);
        break;
      case BOOLEAN:
        mappings = ImmutableMap.of("type", "boolean");
        break;
      case COUNT:
        mappings = ImmutableMap.of("type", "long");
        break;
      case TEXT:
        mappings = ImmutableMap.of("type", "text", "analyzer", "word_delimited");
        break;
      case PATTERN:
        mappings = ImmutableMap.of("type", "text", "analyzer", "pattern");
        break;
      case URN:
        mappings = ImmutableMap.of("type", "text", "analyzer", "urn_component");
        break;
      case PARTIAL:
        mappings = ImmutableMap.of("type", "text", "analyzer", "partial");
        break;
      case PARTIAL_SHORT:
        mappings = ImmutableMap.of("type", "text", "analyzer", "partial_short");
        break;
      case PARTIAL_LONG:
        mappings = ImmutableMap.of("type", "text", "analyzer", "partial_long");
        break;
      case PARTIAL_PATTERN:
        mappings = ImmutableMap.of("type", "text", "analyzer", "partial_pattern");
        break;
      case PARTIAL_URN:
        mappings = ImmutableMap.of("type", "text", "analyzer", "partial_urn_component");
        break;
      case BROWSE_PATH:
        mappings = getMappingsForBrowsePaths();
        break;
      default:
        log.info("Unsupported index type: {}", indexSetting.getIndexType());
        break;
    }
    return Optional.ofNullable(mappings);
  }

  private static Map<String, Object> getMappingsForBrowsePaths() {
    return ImmutableMap.<String, Object>builder().put("type", "text")
        .put("fields", ImmutableMap.of("length",
            ImmutableMap.<String, Object>builder().put("type", "token_count").put("analyzer", "slash_pattern").build()))
        .put("analyzer", "browse_path_hierarchy")
        .put("fielddata", true)
        .build();
  }

}
