package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static com.linkedin.metadata.Constants.ENTITY_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  public static final String SYSTEM_CREATED_FIELD = "systemCreated";

  // Subfields
  public static final String DELIMITED = "delimited";
  public static final String LENGTH = "length";
  public static final String WORD_GRAMS_LENGTH_2 = "wordGrams2";
  public static final String WORD_GRAMS_LENGTH_3 = "wordGrams3";
  public static final String WORD_GRAMS_LENGTH_4 = "wordGrams4";
  public static final Set<String> SUBFIELDS =
      Set.of(
          KEYWORD,
          DELIMITED,
          LENGTH,
          NGRAM,
          WORD_GRAMS_LENGTH_2,
          WORD_GRAMS_LENGTH_3,
          WORD_GRAMS_LENGTH_4);

  // Alias field mappings constants
  public static final String ALIAS_FIELD_TYPE = "alias";
  public static final String PATH = "path";

  public static final String PROPERTIES = "properties";
  public static final String DYNAMIC_TEMPLATES = "dynamic_templates";

  private MappingsBuilder() {}

  /**
   * Builds mappings from entity spec and a collection of structured properties for the entity.
   *
   * @param entitySpec entity's spec
   * @param structuredProperties structured properties for the entity
   * @return mappings
   */
  public static Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final EntitySpec entitySpec,
      Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    Map<String, Object> mappings = getMappings(entityRegistry, entitySpec);

    String entityName = entitySpec.getEntityAnnotation().getName();
    Map<String, Object> structuredPropertiesForEntity =
        getMappingsForStructuredProperty(
            structuredProperties.stream()
                .filter(
                    urnProp -> {
                      try {
                        return urnProp
                            .getSecond()
                            .getEntityTypes()
                            .contains(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + entityName));
                      } catch (URISyntaxException e) {
                        return false;
                      }
                    })
                .collect(Collectors.toSet()));

    if (!structuredPropertiesForEntity.isEmpty()) {
      HashMap<String, Map<String, Object>> props =
          (HashMap<String, Map<String, Object>>)
              ((Map<String, Object>) mappings.get(PROPERTIES))
                  .computeIfAbsent(
                      STRUCTURED_PROPERTY_MAPPING_FIELD,
                      (key) -> new HashMap<>(Map.of(PROPERTIES, new HashMap<>())));

      props.merge(
          PROPERTIES,
          structuredPropertiesForEntity,
          (oldValue, newValue) -> {
            HashMap<String, Object> merged = new HashMap<>(oldValue);
            merged.putAll(newValue);
            return merged.isEmpty() ? null : merged;
          });
    }

    return mappings;
  }

  public static Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull final EntitySpec entitySpec) {
    Map<String, Object> mappings = new HashMap<>();

    entitySpec
        .getSearchableFieldSpecs()
        .forEach(searchableFieldSpec -> mappings.putAll(getMappingsForField(searchableFieldSpec)));
    entitySpec
        .getSearchScoreFieldSpecs()
        .forEach(
            searchScoreFieldSpec ->
                mappings.putAll(getMappingsForSearchScoreField(searchScoreFieldSpec)));
    entitySpec
        .getSearchableRefFieldSpecs()
        .forEach(
            searchableRefFieldSpec ->
                mappings.putAll(
                    getMappingForSearchableRefField(
                        entityRegistry,
                        searchableRefFieldSpec,
                        searchableRefFieldSpec.getSearchableRefAnnotation().getDepth())));
    // Fixed fields
    mappings.put("urn", getMappingsForUrn());
    mappings.put("runId", getMappingsForRunId());
    mappings.put(SYSTEM_CREATED_FIELD, getMappingsForSystemCreated());

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

  private static Map<String, Object> getMappingsForSystemCreated() {
    return ImmutableMap.<String, Object>builder().put(TYPE, ESUtils.DATE_FIELD_TYPE).build();
  }

  public static Map<String, Object> getMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    return properties.stream()
        .map(
            urnProperty -> {
              StructuredPropertyDefinition property = urnProperty.getSecond();
              Map<String, Object> mappingForField = new HashMap<>();
              String valueType = property.getValueType().getId();
              if (valueType.equalsIgnoreCase(LogicalValueType.STRING.name())) {
                mappingForField = getMappingsForKeyword();
              } else if (valueType.equalsIgnoreCase(LogicalValueType.RICH_TEXT.name())) {
                mappingForField = getMappingsForSearchText(FieldType.TEXT_PARTIAL);
              } else if (valueType.equalsIgnoreCase(LogicalValueType.DATE.name())) {
                mappingForField.put(TYPE, ESUtils.DATE_FIELD_TYPE);
              } else if (valueType.equalsIgnoreCase(LogicalValueType.URN.name())) {
                mappingForField = getMappingsForUrn();
              } else if (valueType.equalsIgnoreCase(LogicalValueType.NUMBER.name())) {
                mappingForField.put(TYPE, ESUtils.DOUBLE_FIELD_TYPE);
              }
              return Map.entry(
                  toElasticsearchFieldName(urnProperty.getFirst(), property), mappingForField);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
    } else if (OBJECT_FIELD_TYPES.contains(fieldType)) {
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

    if (ESUtils.getSystemModifiedAtFieldName(searchableFieldSpec).isPresent()) {
      String modifiedAtFieldName = ESUtils.getSystemModifiedAtFieldName(searchableFieldSpec).get();
      mappings.put(modifiedAtFieldName, ImmutableMap.of(TYPE, ESUtils.DATE_FIELD_TYPE));
    }

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

  private static Map<String, Object> getMappingForSearchableRefField(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final SearchableRefFieldSpec searchableRefFieldSpec,
      @Nonnull final int depth) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    Map<String, Object> mappingForProperty = new HashMap<>();
    if (depth == 0) {
      mappings.put(
          searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName(), getMappingsForUrn());
      return mappings;
    }
    String entityType = searchableRefFieldSpec.getSearchableRefAnnotation().getRefType();
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);
    entitySpec
        .getSearchableFieldSpecs()
        .forEach(
            searchableFieldSpec ->
                mappingForField.putAll(getMappingsForField(searchableFieldSpec)));
    entitySpec
        .getSearchableRefFieldSpecs()
        .forEach(
            entitySearchableRefFieldSpec ->
                mappingForField.putAll(
                    getMappingForSearchableRefField(
                        entityRegistry,
                        entitySearchableRefFieldSpec,
                        Math.min(
                            depth - 1,
                            entitySearchableRefFieldSpec
                                .getSearchableRefAnnotation()
                                .getDepth()))));
    mappingForField.put("urn", getMappingsForUrn());
    mappingForProperty.put("properties", mappingForField);
    mappings.put(
        searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName(), mappingForProperty);
    return mappings;
  }

  private static Map<String, Object> getMappingsForFieldNameAliases(
      @Nonnull final SearchableFieldSpec searchableFieldSpec) {
    Map<String, Object> mappings = new HashMap<>();
    List<String> fieldNameAliases =
        searchableFieldSpec.getSearchableAnnotation().getFieldNameAliases();
    fieldNameAliases.forEach(
        alias -> {
          Map<String, Object> aliasMappings = new HashMap<>();
          aliasMappings.put(TYPE, ALIAS_FIELD_TYPE);
          aliasMappings.put(PATH, searchableFieldSpec.getSearchableAnnotation().getFieldName());
          mappings.put(alias, aliasMappings);
        });
    return mappings;
  }
}
