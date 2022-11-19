package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;


@Slf4j
public class MappingsBuilder {

  private MappingsBuilder() {
  }

  public static Map<String, Object> getMappings(@Nonnull final EntitySpec entitySpec) {
    Map<String, Object> mappings = new HashMap<>();

    entitySpec.getSearchableFieldSpecs()
        .forEach(searchableFieldSpec -> mappings.putAll(getMappingsForField(searchableFieldSpec)));
    entitySpec.getSearchScoreFieldSpecs()
        .forEach(searchScoreFieldSpec -> mappings.putAll(getMappingsForSearchScoreField(searchScoreFieldSpec)));

    // Fixed fields
    mappings.put("urn", getMappingsForUrn());
    mappings.put("runId", getMappingsForRunId());

    return ImmutableMap.of("properties", mappings);
  }

  private static Map<String, Object> getMappingsForUrn() {
    Map<String, Object> subFields = new HashMap<>();
    subFields.put("delimited", ImmutableMap.of(
            "type", "text",
            "analyzer", URN_ANALYZER,
            "search_analyzer", URN_SEARCH_ANALYZER)
    );
    subFields.put("ngram", ImmutableMap.of(
            "type", "search_as_you_type",
            "max_shingle_size", "4"));
    return ImmutableMap.<String, Object>builder()
            .put("type", "keyword")
            .put("fields", subFields)
            .build();
  }

  private static Map<String, Object> getMappingsForRunId() {
    return ImmutableMap.<String, Object>builder().put("type", "keyword").build();
  }

  private static Map<String, Object> getMappingsForField(@Nonnull final SearchableFieldSpec searchableFieldSpec) {
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();
    boolean addToFilters = searchableFieldSpec.getSearchableAnnotation().isAddToFilters();

    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    if (fieldType == FieldType.KEYWORD) {
      mappingForField.put("type", "keyword");
      mappingForField.put("normalizer", "keyword_normalizer");
      // Add keyword subfield without lowercase filter
      mappingForField.put("fields", ImmutableMap.of("keyword", ImmutableMap.of("type", "keyword")));
    } else if (fieldType == FieldType.TEXT || fieldType == FieldType.TEXT_PARTIAL) {
      mappingForField.put("type", "keyword");
      mappingForField.put("normalizer", "keyword_normalizer");
      Map<String, Object> subFields = new HashMap<>();
      if (fieldType == FieldType.TEXT_PARTIAL) {
        subFields.put("ngram", ImmutableMap.of(
                "type", "search_as_you_type",
                "max_shingle_size", "4"));
      }
      subFields.put("delimited", ImmutableMap.of(
              "type", "text",
              "analyzer", TEXT_ANALYZER,
              "search_analyzer", TEXT_SEARCH_ANALYZER));
      // Add keyword subfield without lowercase filter
      subFields.put("keyword", ImmutableMap.of("type", "keyword"));
      mappingForField.put("fields", subFields);
    } else if (fieldType == FieldType.BROWSE_PATH) {
      mappingForField.put("type", "text");
      mappingForField.put("fields",
          ImmutableMap.of("length", ImmutableMap.of("type", "token_count", "analyzer", "slash_pattern")));
      mappingForField.put("analyzer", "browse_path_hierarchy");
      mappingForField.put("fielddata", true);
    } else if (fieldType == FieldType.URN || fieldType == FieldType.URN_PARTIAL) {
      mappingForField.put("type", "text");
      mappingForField.put("analyzer", URN_ANALYZER);
      mappingForField.put("search_analyzer", URN_SEARCH_ANALYZER);
      Map<String, Object> subFields = new HashMap<>();
      if (fieldType == FieldType.URN_PARTIAL) {
        subFields.put("ngram", ImmutableMap.of(
                "type", "search_as_you_type",
                "max_shingle_size", "4"));
      }
      subFields.put("keyword", ImmutableMap.of("type", "keyword"));
      mappingForField.put("fields", subFields);
    } else if (fieldType == FieldType.BOOLEAN) {
      mappingForField.put("type", "boolean");
    } else if (fieldType == FieldType.COUNT) {
      mappingForField.put("type", "long");
    } else if (fieldType == FieldType.DATETIME) {
      mappingForField.put("type", "date");
    } else if (fieldType == FieldType.OBJECT) {
      mappingForField.put("type", "object");
    } else {
      log.info("FieldType {} has no mappings implemented", fieldType);
    }
    mappings.put(searchableFieldSpec.getSearchableAnnotation().getFieldName(), mappingForField);

    searchableFieldSpec.getSearchableAnnotation()
        .getHasValuesFieldName()
        .ifPresent(fieldName -> mappings.put(fieldName, ImmutableMap.of("type", "boolean")));
    searchableFieldSpec.getSearchableAnnotation()
        .getNumValuesFieldName()
        .ifPresent(fieldName -> mappings.put(fieldName, ImmutableMap.of("type", "long")));

    return mappings;
  }

  private static Map<String, Object> getMappingsForSearchScoreField(
      @Nonnull final SearchScoreFieldSpec searchScoreFieldSpec) {
    return ImmutableMap.of(searchScoreFieldSpec.getSearchScoreAnnotation().getFieldName(),
        ImmutableMap.of("type", "double"));
  }
}
