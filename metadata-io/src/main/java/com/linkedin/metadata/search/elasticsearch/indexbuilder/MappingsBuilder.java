package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MappingsBuilder {

  private static final Map<String, String> PARTIAL_NGRAM_CONFIG =
      ImmutableMap.of(
          TYPE, "search_as_you_type",
          MAX_SHINGLE_SIZE, "4",
          DOC_VALUES, "false");

  public static Map<String, String> getPartialNgramConfigWithOverrides(
      Map<String, String> overrides) {
    return Stream.concat(PARTIAL_NGRAM_CONFIG.entrySet().stream(), overrides.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static final Map<String, String> KEYWORD_TYPE_MAP = ImmutableMap.of(TYPE, KEYWORD);

  // Subfields
  public static final String DELIMITED = "delimited";
  public static final String LENGTH = "length";
  public static final String WORD_GRAMS_LENGTH_2 = "wordGrams2";
  public static final String WORD_GRAMS_LENGTH_3 = "wordGrams3";
  public static final String WORD_GRAMS_LENGTH_4 = "wordGrams4";

  // Alias field mappings constants
  public static final String ALIAS = "alias";
  public static final String PATH = "path";

  public static final String PROPERTIES = "properties";

  private MappingsBuilder() {}

  public static Map<String, Object> getMappings(@Nonnull final EntitySpec entitySpec) {
    Map<String, Object> mappings = new HashMap<>();

    entitySpec
        .getSearchableFieldSpecs()
        .forEach(searchableFieldSpec -> mappings.putAll(getMappingsForField(searchableFieldSpec)));
    entitySpec
        .getSearchScoreFieldSpecs()
        .forEach(
            searchScoreFieldSpec ->
                mappings.putAll(getMappingsForSearchScoreField(searchScoreFieldSpec)));

    // Fixed fields
    mappings.put("urn", getMappingsForUrn());
    mappings.put("runId", getMappingsForRunId());

    return ImmutableMap.of(PROPERTIES, mappings);
  }

  private static Map<String, Object> getMappingsForUrn() {
    Map<String, Object> subFields = new HashMap<>();
    subFields.put(
        DELIMITED,
        ImmutableMap.of(
            TYPE, ESUtils.TEXT_FIELD_TYPE,
            ANALYZER, URN_ANALYZER,
            SEARCH_ANALYZER, URN_SEARCH_ANALYZER,
            SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER));
    subFields.put(
        NGRAM,
        getPartialNgramConfigWithOverrides(ImmutableMap.of(ANALYZER, PARTIAL_URN_COMPONENT)));
    return ImmutableMap.<String, Object>builder()
        .put(TYPE, ESUtils.KEYWORD_FIELD_TYPE)
        .put(FIELDS, subFields)
        .build();
  }

  private static Map<String, Object> getMappingsForRunId() {
    return ImmutableMap.<String, Object>builder().put(TYPE, ESUtils.KEYWORD_FIELD_TYPE).build();
  }

  private static Map<String, Object> getMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec) {
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();

    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    if (fieldType == FieldType.KEYWORD) {
      mappingForField.putAll(getMappingsForKeyword());
    } else if (fieldType == FieldType.TEXT
        || fieldType == FieldType.TEXT_PARTIAL
        || fieldType == FieldType.WORD_GRAM) {
      mappingForField.putAll(getMappingsForSearchText(fieldType));
    } else if (fieldType == FieldType.BROWSE_PATH) {
      mappingForField.put(TYPE, ESUtils.TEXT_FIELD_TYPE);
      mappingForField.put(
          FIELDS,
          ImmutableMap.of(
              LENGTH,
              ImmutableMap.of(
                  TYPE, ESUtils.TOKEN_COUNT_FIELD_TYPE, ANALYZER, SLASH_PATTERN_ANALYZER)));
      mappingForField.put(ANALYZER, BROWSE_PATH_HIERARCHY_ANALYZER);
      mappingForField.put(FIELDDATA, true);
    } else if (fieldType == FieldType.BROWSE_PATH_V2) {
      mappingForField.put(TYPE, ESUtils.TEXT_FIELD_TYPE);
      mappingForField.put(
          FIELDS,
          ImmutableMap.of(
              LENGTH,
              ImmutableMap.of(
                  TYPE,
                  ESUtils.TOKEN_COUNT_FIELD_TYPE,
                  ANALYZER,
                  UNIT_SEPARATOR_PATTERN_ANALYZER)));
      mappingForField.put(ANALYZER, BROWSE_PATH_V2_HIERARCHY_ANALYZER);
      mappingForField.put(FIELDDATA, true);
    } else if (fieldType == FieldType.URN || fieldType == FieldType.URN_PARTIAL) {
      mappingForField.put(TYPE, ESUtils.TEXT_FIELD_TYPE);
      mappingForField.put(ANALYZER, URN_ANALYZER);
      mappingForField.put(SEARCH_ANALYZER, URN_SEARCH_ANALYZER);
      mappingForField.put(SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER);
      Map<String, Object> subFields = new HashMap<>();
      if (fieldType == FieldType.URN_PARTIAL) {
        subFields.put(
            NGRAM, getPartialNgramConfigWithOverrides(Map.of(ANALYZER, PARTIAL_URN_COMPONENT)));
      }
      subFields.put(KEYWORD, KEYWORD_TYPE_MAP);
      mappingForField.put(FIELDS, subFields);
    } else if (fieldType == FieldType.BOOLEAN) {
      mappingForField.put(TYPE, ESUtils.BOOLEAN_FIELD_TYPE);
    } else if (fieldType == FieldType.COUNT) {
      mappingForField.put(TYPE, ESUtils.LONG_FIELD_TYPE);
    } else if (fieldType == FieldType.DATETIME) {
      mappingForField.put(TYPE, ESUtils.DATE_FIELD_TYPE);
    } else if (fieldType == FieldType.OBJECT) {
      mappingForField.put(TYPE, ESUtils.OBJECT_FIELD_TYPE);
    } else if (fieldType == FieldType.DOUBLE) {
      mappingForField.put(TYPE, ESUtils.DOUBLE_FIELD_TYPE);
    } else {
      log.info("FieldType {} has no mappings implemented", fieldType);
    }
    mappings.put(searchableFieldSpec.getSearchableAnnotation().getFieldName(), mappingForField);

    searchableFieldSpec
        .getSearchableAnnotation()
        .getHasValuesFieldName()
        .ifPresent(
            fieldName ->
                mappings.put(fieldName, ImmutableMap.of(TYPE, ESUtils.BOOLEAN_FIELD_TYPE)));
    searchableFieldSpec
        .getSearchableAnnotation()
        .getNumValuesFieldName()
        .ifPresent(
            fieldName -> mappings.put(fieldName, ImmutableMap.of(TYPE, ESUtils.LONG_FIELD_TYPE)));
    mappings.putAll(getMappingsForFieldNameAliases(searchableFieldSpec));

    return mappings;
  }

  private static Map<String, Object> getMappingsForKeyword() {
    Map<String, Object> mappingForField = new HashMap<>();
    mappingForField.put(TYPE, ESUtils.KEYWORD_FIELD_TYPE);
    mappingForField.put(NORMALIZER, KEYWORD_NORMALIZER);
    // Add keyword subfield without lowercase filter
    mappingForField.put(FIELDS, ImmutableMap.of(KEYWORD, KEYWORD_TYPE_MAP));
    return mappingForField;
  }

  private static Map<String, Object> getMappingsForSearchText(FieldType fieldType) {
    Map<String, Object> mappingForField = new HashMap<>();
    mappingForField.put(TYPE, ESUtils.KEYWORD_FIELD_TYPE);
    mappingForField.put(NORMALIZER, KEYWORD_NORMALIZER);
    Map<String, Object> subFields = new HashMap<>();
    if (fieldType == FieldType.TEXT_PARTIAL || fieldType == FieldType.WORD_GRAM) {
      subFields.put(
          NGRAM, getPartialNgramConfigWithOverrides(ImmutableMap.of(ANALYZER, PARTIAL_ANALYZER)));
      if (fieldType == FieldType.WORD_GRAM) {
        for (Map.Entry<String, String> entry :
            Map.of(
                    WORD_GRAMS_LENGTH_2, WORD_GRAM_2_ANALYZER,
                    WORD_GRAMS_LENGTH_3, WORD_GRAM_3_ANALYZER,
                    WORD_GRAMS_LENGTH_4, WORD_GRAM_4_ANALYZER)
                .entrySet()) {
          String fieldName = entry.getKey();
          String analyzerName = entry.getValue();
          subFields.put(
              fieldName, ImmutableMap.of(TYPE, ESUtils.TEXT_FIELD_TYPE, ANALYZER, analyzerName));
        }
      }
    }
    subFields.put(
        DELIMITED,
        ImmutableMap.of(
            TYPE, ESUtils.TEXT_FIELD_TYPE,
            ANALYZER, TEXT_ANALYZER,
            SEARCH_ANALYZER, TEXT_SEARCH_ANALYZER,
            SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER));
    // Add keyword subfield without lowercase filter
    subFields.put(KEYWORD, KEYWORD_TYPE_MAP);
    mappingForField.put(FIELDS, subFields);
    return mappingForField;
  }

  private static Map<String, Object> getMappingsForSearchScoreField(
      @Nonnull final SearchScoreFieldSpec searchScoreFieldSpec) {
    return ImmutableMap.of(
        searchScoreFieldSpec.getSearchScoreAnnotation().getFieldName(),
        ImmutableMap.of(TYPE, ESUtils.DOUBLE_FIELD_TYPE));
  }

  private static Map<String, Object> getMappingsForFieldNameAliases(
      @Nonnull final SearchableFieldSpec searchableFieldSpec) {
    Map<String, Object> mappings = new HashMap<>();
    List<String> fieldNameAliases =
        searchableFieldSpec.getSearchableAnnotation().getFieldNameAliases();
    fieldNameAliases.forEach(
        alias -> {
          Map<String, Object> aliasMappings = new HashMap<>();
          aliasMappings.put(TYPE, ALIAS);
          aliasMappings.put(PATH, searchableFieldSpec.getSearchableAnnotation().getFieldName());
          mappings.put(alias, aliasMappings);
        });
    return mappings;
  }
}
