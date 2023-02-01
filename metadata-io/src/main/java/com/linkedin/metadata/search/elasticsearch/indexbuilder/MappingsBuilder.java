package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;


@Slf4j
public class MappingsBuilder {

  private static final Map<String, String> PARTIAL_NGRAM_CONFIG = ImmutableMap.of(
          TYPE, "search_as_you_type",
          MAX_SHINGLE_SIZE, "4",
          DOC_VALUES, "false");

  public static Map<String, String> getPartialNgramConfigWithOverrides(Map<String, String> overrides) {
    return Stream.concat(PARTIAL_NGRAM_CONFIG.entrySet().stream(), overrides.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static final Map<String, String> KEYWORD_TYPE_MAP = ImmutableMap.of(TYPE, KEYWORD);

  // Field Types
  public static final String BOOLEAN = "boolean";
  public static final String DATE = "date";
  public static final String DOUBLE = "double";
  public static final String LONG = "long";
  public static final String OBJECT = "object";
  public static final String TEXT = "text";
  public static final String TOKEN_COUNT = "token_count";

  // Subfields
  public static final String DELIMITED = "delimited";
  public static final String LENGTH = "length";

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
    subFields.put(DELIMITED, ImmutableMap.of(
            TYPE, TEXT,
            ANALYZER, URN_ANALYZER,
            SEARCH_ANALYZER, URN_SEARCH_ANALYZER,
            SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER)
    );
    subFields.put(NGRAM, getPartialNgramConfigWithOverrides(
            ImmutableMap.of(
                    ANALYZER, PARTIAL_URN_COMPONENT
            )
    ));
    return ImmutableMap.<String, Object>builder()
            .put(TYPE, KEYWORD)
            .put(FIELDS, subFields)
            .build();
  }

  private static Map<String, Object> getMappingsForRunId() {
    return ImmutableMap.<String, Object>builder().put(TYPE, KEYWORD).build();
  }

  private static Map<String, Object> getMappingsForField(@Nonnull final SearchableFieldSpec searchableFieldSpec) {
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();

    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    if (fieldType == FieldType.KEYWORD) {
      mappingForField.put(TYPE, KEYWORD);
      mappingForField.put(NORMALIZER, KEYWORD_NORMALIZER);
      // Add keyword subfield without lowercase filter
      mappingForField.put(FIELDS, ImmutableMap.of(KEYWORD, KEYWORD_TYPE_MAP));
    } else if (fieldType == FieldType.TEXT || fieldType == FieldType.TEXT_PARTIAL) {
      mappingForField.put(TYPE, KEYWORD);
      mappingForField.put(NORMALIZER, KEYWORD_NORMALIZER);
      Map<String, Object> subFields = new HashMap<>();
      if (fieldType == FieldType.TEXT_PARTIAL) {
        subFields.put(NGRAM, getPartialNgramConfigWithOverrides(
                ImmutableMap.of(
                        ANALYZER, PARTIAL_ANALYZER
                )
        ));
      }
      subFields.put(DELIMITED, ImmutableMap.of(
              TYPE, TEXT,
              ANALYZER, TEXT_ANALYZER,
              SEARCH_ANALYZER, TEXT_SEARCH_ANALYZER,
              SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER));
      // Add keyword subfield without lowercase filter
      subFields.put(KEYWORD, KEYWORD_TYPE_MAP);
      mappingForField.put(FIELDS, subFields);
    } else if (fieldType == FieldType.BROWSE_PATH) {
      mappingForField.put(TYPE, TEXT);
      mappingForField.put(FIELDS,
          ImmutableMap.of(LENGTH, ImmutableMap.of(
              TYPE, TOKEN_COUNT,
              ANALYZER, SLASH_PATTERN_ANALYZER)));
      mappingForField.put(ANALYZER, BROWSE_PATH_HIERARCHY_ANALYZER);
      mappingForField.put(FIELDDATA, true);
    } else if (fieldType == FieldType.URN || fieldType == FieldType.URN_PARTIAL) {
      mappingForField.put(TYPE, TEXT);
      mappingForField.put(ANALYZER, URN_ANALYZER);
      mappingForField.put(SEARCH_ANALYZER, URN_SEARCH_ANALYZER);
      mappingForField.put(SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER);
      Map<String, Object> subFields = new HashMap<>();
      if (fieldType == FieldType.URN_PARTIAL) {
        subFields.put(NGRAM, getPartialNgramConfigWithOverrides(
                Map.of(
                        ANALYZER, PARTIAL_URN_COMPONENT
                )
        ));
      }
      subFields.put(KEYWORD, KEYWORD_TYPE_MAP);
      mappingForField.put(FIELDS, subFields);
    } else if (fieldType == FieldType.BOOLEAN) {
      mappingForField.put(TYPE, BOOLEAN);
    } else if (fieldType == FieldType.COUNT) {
      mappingForField.put(TYPE, LONG);
    } else if (fieldType == FieldType.DATETIME) {
      mappingForField.put(TYPE, DATE);
    } else if (fieldType == FieldType.OBJECT) {
      mappingForField.put(TYPE, OBJECT);
    } else {
      log.info("FieldType {} has no mappings implemented", fieldType);
    }
    mappings.put(searchableFieldSpec.getSearchableAnnotation().getFieldName(), mappingForField);

    searchableFieldSpec.getSearchableAnnotation()
        .getHasValuesFieldName()
        .ifPresent(fieldName -> mappings.put(fieldName, ImmutableMap.of(TYPE, BOOLEAN)));
    searchableFieldSpec.getSearchableAnnotation()
        .getNumValuesFieldName()
        .ifPresent(fieldName -> mappings.put(fieldName, ImmutableMap.of(TYPE, LONG)));

    return mappings;
  }

  private static Map<String, Object> getMappingsForSearchScoreField(
      @Nonnull final SearchScoreFieldSpec searchScoreFieldSpec) {
    return ImmutableMap.of(searchScoreFieldSpec.getSearchScoreAnnotation().getFieldName(),
        ImmutableMap.of(TYPE, DOUBLE));
  }
}
