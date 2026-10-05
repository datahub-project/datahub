package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.CUSTOM_QUOTE_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDS;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NGRAM;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.PARTIAL_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.PARTIAL_URN_COMPONENT;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_QUOTE_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.TEXT_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.URN_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.URN_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.WORD_GRAM_2_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.WORD_GRAM_3_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.WORD_GRAM_4_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.DELIMITED;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_2;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_3;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_4;
import static com.linkedin.metadata.search.utils.ESUtils.BOOLEAN_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DATE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DOUBLE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.FLOAT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.IGNORE_ABOVE;
import static com.linkedin.metadata.search.utils.ESUtils.INTEGER_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_MAXLENGTH;
import static com.linkedin.metadata.search.utils.ESUtils.LONG_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.OBJECT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.keywordIgnoreAboveForMaxBytes;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for mapping DataHub field types to Elasticsearch field types and configurations.
 * This class centralizes all field type mapping logic to improve maintainability.
 */
@Slf4j
public class FieldTypeMapper {
  static final Map<String, String> DEFAULT_PARTIAL_NGRAM_CONFIG =
      OpenSearchSearchClientShim.PARTIAL_NGRAM_CONFIG;

  private static final Set<FieldType> URN_FIELD_TYPES =
      Set.of(FieldType.URN, FieldType.URN_PARTIAL);

  private FieldTypeMapper() {
    // Utility class - prevent instantiation
  }

  /**
   * Converts a FieldType to the corresponding Elasticsearch type.
   *
   * @param fieldType the DataHub field type
   * @return the Elasticsearch type string
   */
  @Nonnull
  public static String getElasticsearchTypeForFieldType(@Nonnull FieldType fieldType) {
    switch (fieldType) {
      case KEYWORD:
        return KEYWORD_FIELD_TYPE;
      case TEXT:
      case TEXT_PARTIAL:
        return KEYWORD_FIELD_TYPE; // Treat text fields as keyword for simplicity
      case BOOLEAN:
        return BOOLEAN_FIELD_TYPE;
      case COUNT:
        return LONG_FIELD_TYPE;
      case DATETIME:
        return DATE_FIELD_TYPE;
      case DOUBLE:
        return DOUBLE_FIELD_TYPE;
      case URN:
        return KEYWORD_FIELD_TYPE; // URN fields are treated as keyword
      case MAP_ARRAY:
        return OBJECT_FIELD_TYPE; // MAP_ARRAY fields are stored as dynamic objects
      case BROWSE_PATH_V2:
        return "text"; // BROWSE_PATH_V2 fields use text type with special analyzer
      default:
        if (OBJECT_FIELD_TYPES.contains(fieldType)) {
          return OBJECT_FIELD_TYPE;
        }
        log.debug("FieldType {} not supported, defaulting to keyword", fieldType);
        return KEYWORD_FIELD_TYPE;
    }
  }

  /**
   * Converts a FieldType to the corresponding Elasticsearch type, considering the underlying PDL
   * field type for more precise numeric type mapping.
   *
   * @param fieldType the DataHub field type
   * @param searchableFieldSpec the searchable field spec containing the underlying PDL schema
   * @return the Elasticsearch type string
   */
  @Nonnull
  public static String getElasticsearchTypeForFieldType(
      @Nonnull FieldType fieldType, @Nonnull SearchableFieldSpec searchableFieldSpec) {
    // For COUNT fields, try to determine the appropriate numeric type based on the underlying PDL
    // field type
    if (fieldType == FieldType.COUNT) {
      return getElasticsearchTypeForCountField(searchableFieldSpec);
    }

    // For all other field types, use the standard mapping
    return getElasticsearchTypeForFieldType(fieldType);
  }

  /**
   * Determines the appropriate Elasticsearch numeric type for a COUNT field based on the underlying
   * PDL field type.
   *
   * @param searchableFieldSpec the searchable field spec containing the underlying PDL schema
   * @return the Elasticsearch type string
   */
  @Nonnull
  private static String getElasticsearchTypeForCountField(
      @Nonnull SearchableFieldSpec searchableFieldSpec) {
    DataSchema pegasusSchema = searchableFieldSpec.getPegasusSchema();

    // If the schema is primitive, we can determine the exact numeric type
    if (pegasusSchema.isPrimitive()) {
      PrimitiveDataSchema primitiveSchema = (PrimitiveDataSchema) pegasusSchema;
      DataSchema.Type schemaType = primitiveSchema.getType();

      switch (schemaType) {
        case INT:
          return INTEGER_FIELD_TYPE;
        case LONG:
          return LONG_FIELD_TYPE;
        case FLOAT:
          return FLOAT_FIELD_TYPE;
        case DOUBLE:
          return DOUBLE_FIELD_TYPE;
        default:
          // For non-numeric primitive types, default to long for COUNT fields
          log.debug(
              "Non-numeric primitive type {} for COUNT field, defaulting to long", schemaType);
          return LONG_FIELD_TYPE;
      }
    }

    // For non-primitive schemas, default to long for COUNT fields
    log.debug("Non-primitive schema for COUNT field, defaulting to long");
    return LONG_FIELD_TYPE;
  }

  /**
   * Creates a mapping configuration for a keyword field.
   *
   * @return mapping configuration for keyword field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForKeyword() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    mapping.put(FIELDS, Map.of(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE)));
    return mapping;
  }

  /**
   * Creates a keyword mapping guarded by a byte-safe {@code ignore_above} at the Lucene keyword
   * term limit. Delegates to {@link #getMappingsForKeywordWithIgnoreAbove(int)}.
   *
   * <p>Includes a {@code .keyword} multi-field so filters/facets that append {@code .keyword} (e.g.
   * STRING/RICH_TEXT structured properties via {@code usesKeywordSubfield}) resolve.
   */
  @Nonnull
  public static Map<String, Object> getMappingsForKeywordWithIgnoreAbove() {
    return getMappingsForKeywordWithIgnoreAbove(KEYWORD_MAXLENGTH);
  }

  /**
   * @param keywordMaxBytes Lucene keyword term limit in UTF-8 bytes (e.g. configured structured
   *     property max). Converted to a byte-safe character {@code ignore_above}.
   */
  @Nonnull
  public static Map<String, Object> getMappingsForKeywordWithIgnoreAbove(int keywordMaxBytes) {
    int ignoreAbove = keywordIgnoreAboveForMaxBytes(keywordMaxBytes);
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    mapping.put(IGNORE_ABOVE, ignoreAbove);
    // Subfield mirrors parent ignore_above so exact-match / aggregation queries on .keyword
    // are protected from Lucene term-length failures on oversized values.
    mapping.put(
        FIELDS, Map.of(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE, IGNORE_ABOVE, ignoreAbove)));
    return mapping;
  }

  /**
   * Creates a mapping configuration for a structured-property URN field.
   *
   * <p>Parent keyword only (no {@code .keyword} / delimited / ngram subfields): query-time
   * structured-property filters skip {@code .keyword} for URN value types via {@code
   * StructuredPropertyUtils.usesKeywordSubfield}. Entity searchable URN fields use {@link
   * #getMappingsForSearchableUrn} / the overload with partial ngram config instead.
   *
   * @return mapping configuration for URN structured property field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForUrn() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(IGNORE_ABOVE, 255);
    return mapping;
  }

  @Nonnull
  public static Map<String, Object> getMappingsForUrn(
      @Nonnull Map<String, String> partialNgramConfig) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(
        FIELDS,
        Map.of(
            DELIMITED,
            Map.of(
                "type",
                ESUtils.TEXT_FIELD_TYPE,
                ANALYZER,
                URN_ANALYZER,
                SEARCH_ANALYZER,
                URN_SEARCH_ANALYZER,
                SEARCH_QUOTE_ANALYZER,
                CUSTOM_QUOTE_ANALYZER),
            NGRAM,
            partialNgramConfigWithOverrides(
                partialNgramConfig, Map.of(ANALYZER, PARTIAL_URN_COMPONENT))));
    return mapping;
  }

  /**
   * Creates a mapping configuration for dynamic object fields. This creates dynamic object mappings
   * with dynamic: true to allow flexible data structures while still supporting aliases.
   *
   * @return mapping configuration for dynamic object field
   */
  @Nonnull
  public static Map<String, Object> getDynamicObjectMappings() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", OBJECT_FIELD_TYPE);
    mapping.put("dynamic", true);
    // Add an empty properties structure to ensure the field is not completely empty
    // This allows aliases to point to the field while still maintaining dynamic behavior
    mapping.put("properties", new HashMap<String, Object>());
    return mapping;
  }

  /**
   * Creates a mapping configuration for a field based on its type. This method handles all field
   * types and returns the appropriate mapping.
   *
   * @param fieldType the DataHub field type
   * @return mapping configuration for the field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForFieldType(@Nonnull FieldType fieldType) {
    return getMappingsForFieldType(fieldType, DEFAULT_PARTIAL_NGRAM_CONFIG);
  }

  @Nonnull
  public static Map<String, Object> getMappingsForFieldType(
      @Nonnull FieldType fieldType, @Nonnull Map<String, String> partialNgramConfig) {
    switch (fieldType) {
      case KEYWORD:
        return getMappingsForKeyword();
      case TEXT:
      case TEXT_PARTIAL:
      case WORD_GRAM:
        return getMappingsForSearchText(fieldType, partialNgramConfig);
      case BOOLEAN:
        return Map.of("type", BOOLEAN_FIELD_TYPE);
      case COUNT:
        return Map.of("type", LONG_FIELD_TYPE);
      case DATETIME:
        return Map.of("type", DATE_FIELD_TYPE);
      case DOUBLE:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      case URN:
      case URN_PARTIAL:
        return getMappingsForSearchableUrn(fieldType, partialNgramConfig);
      case BROWSE_PATH_V2:
        return getMappingsForBrowsePathV2();
      default:
        if (OBJECT_FIELD_TYPES.contains(fieldType)) {
          return getDynamicObjectMappings();
        }
        log.debug("FieldType {} not supported, defaulting to keyword", fieldType);
        return getMappingsForKeyword();
    }
  }

  /**
   * Creates a mapping configuration for a field based on its type, considering the underlying PDL
   * field type for more precise numeric type mapping.
   *
   * @param fieldType the DataHub field type
   * @param searchableFieldSpec the searchable field spec containing the underlying PDL schema
   * @return mapping configuration for the field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForFieldType(
      @Nonnull FieldType fieldType, @Nonnull SearchableFieldSpec searchableFieldSpec) {
    return getMappingsForFieldType(fieldType, searchableFieldSpec, DEFAULT_PARTIAL_NGRAM_CONFIG);
  }

  @Nonnull
  public static Map<String, Object> getMappingsForFieldType(
      @Nonnull FieldType fieldType,
      @Nonnull SearchableFieldSpec searchableFieldSpec,
      @Nonnull Map<String, String> partialNgramConfig) {

    // For COUNT fields, try to determine the appropriate numeric type based on the underlying PDL
    // field type
    if (fieldType == FieldType.COUNT) {
      String elasticsearchType = getElasticsearchTypeForCountField(searchableFieldSpec);
      return Map.of("type", elasticsearchType);
    }

    // For all other field types, use the standard mapping
    return getMappingsForFieldType(fieldType, partialNgramConfig);
  }

  /**
   * Creates the mapping for a field's copy under {@code _aspects.<aspect>}. Full-text search reads
   * the root projection fields, so the aspect copy is never analyzed: string values map to keywords
   * guarded by {@code ignore_above}, and other types keep their standard mapping.
   *
   * @param fieldType the DataHub field type
   * @param searchableFieldSpec the searchable field spec containing the underlying PDL schema
   * @return mapping configuration for the aspect copy of the field
   */
  @Nonnull
  public static Map<String, Object> getAspectMappingsForFieldType(
      @Nonnull FieldType fieldType, @Nonnull SearchableFieldSpec searchableFieldSpec) {
    switch (fieldType) {
      case KEYWORD:
      case TEXT:
      case TEXT_PARTIAL:
      case WORD_GRAM:
      case BROWSE_PATH:
      case BROWSE_PATH_V2:
        return getMappingsForKeywordWithIgnoreAbove();
      case URN:
      case URN_PARTIAL:
        return getMappingsForUrn();
      default:
        return getMappingsForFieldType(fieldType, searchableFieldSpec);
    }
  }

  @Nonnull
  public static Map<String, Object> getRichestCompatibleMapping(
      @Nonnull List<SearchableFieldSpec> sourceFieldSpecs,
      @Nonnull Map<String, String> partialNgramConfig) {
    SearchableFieldSpec representative =
        sourceFieldSpecs.stream()
            .max(
                Comparator.comparingInt(FieldTypeMapper::mappingRichness)
                    // Break richness ties deterministically so the emitted mapping does not
                    // depend on entity-spec iteration order across builds.
                    .thenComparing(spec -> spec.getSearchableAnnotation().getFieldType().name())
                    .thenComparing(spec -> String.valueOf(spec.getPath())))
            .orElseThrow(() -> new IllegalArgumentException("sourceFieldSpecs must not be empty"));
    FieldType representativeType = representative.getSearchableAnnotation().getFieldType();
    Set<FieldType> fieldTypes =
        sourceFieldSpecs.stream()
            .map(spec -> spec.getSearchableAnnotation().getFieldType())
            .collect(Collectors.toSet());
    // A text field with partial or word-gram analysis is richer than a URN field, so it keeps its
    // own mapping, which indexes the URN values analyzed too
    if (URN_FIELD_TYPES.contains(representativeType) && !URN_FIELD_TYPES.containsAll(fieldTypes)) {
      log.warn(
          "Root field {} is {} across the entities of one index; it gets a keyword base with URN"
              + " analysis on .delimited, so full-text search on it can miss values of any of"
              + " these entities",
          representative.getSearchableAnnotation().getFieldName(),
          fieldTypes);
      return getMappingsForUrnSharedWithKeyword(representativeType, partialNgramConfig);
    }
    return getMappingsForFieldType(representativeType, representative, partialNgramConfig);
  }

  /**
   * Creates a mapping configuration for a field based on its SearchableFieldSpec. This method
   * handles field name conflicts and creates the appropriate mapping.
   *
   * @param searchableFieldSpec the searchable field spec
   * @return mapping configuration for the field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForField(
      @Nonnull SearchableFieldSpec searchableFieldSpec) {

    String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();

    // Use the enhanced mapping that considers the underlying PDL field type
    Map<String, Object> mapping =
        new HashMap<>(getMappingsForFieldType(fieldType, searchableFieldSpec));

    // Note: copy_to is handled at the root field level, not at the aspect field level

    return new HashMap<>(Map.of(fieldName, mapping));
  }

  /**
   * Creates a mapping configuration for a field based on its SearchableFieldSpec. This is a
   * convenience method without conflict handling.
   *
   * @param searchableFieldSpec the searchable field spec
   * @param aspectName the aspect name
   * @return mapping configuration for the field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForField(
      @Nonnull SearchableFieldSpec searchableFieldSpec, @Nonnull String aspectName) {
    return MultiEntityMappingsBuilder.getMappingsForField(
        searchableFieldSpec, aspectName, null, null);
  }

  /**
   * Converts a LogicalValueType to the corresponding Elasticsearch type.
   *
   * @param valueType the logical value type
   * @return the Elasticsearch type string
   */
  @Nonnull
  public static String getElasticsearchTypeForLogicalValueType(
      @Nonnull LogicalValueType valueType) {
    switch (valueType) {
      case STRING:
      case RICH_TEXT:
        return KEYWORD_FIELD_TYPE;
      case DATE:
        return DATE_FIELD_TYPE;
      case URN:
        return KEYWORD_FIELD_TYPE;
      case NUMBER:
        return DOUBLE_FIELD_TYPE;
      default:
        log.debug("LogicalValueType {} not supported, defaulting to keyword", valueType);
        return KEYWORD_FIELD_TYPE;
    }
  }

  /**
   * Creates a mapping configuration for a LogicalValueType.
   *
   * @param valueType the logical value type
   * @return mapping configuration for the value type
   */
  @Nonnull
  public static Map<String, Object> getMappingsForLogicalValueType(
      @Nonnull LogicalValueType valueType) {
    return getMappingsForLogicalValueType(valueType, KEYWORD_MAXLENGTH);
  }

  @Nonnull
  public static Map<String, Object> getMappingsForLogicalValueType(
      @Nonnull LogicalValueType valueType, int keywordMaxLength) {
    switch (valueType) {
      case STRING:
      case RICH_TEXT:
        // ignore_above protects reindex from pre-existing values over the keyword limit;
        // StructuredPropertiesValidator rejects new oversized writes. .keyword multi-field
        // matches usesKeywordSubfield query/facet resolution.
        return getMappingsForKeywordWithIgnoreAbove(keywordMaxLength);
      case DATE:
        return Map.of("type", DATE_FIELD_TYPE);
      case URN:
        return getMappingsForUrn();
      case NUMBER:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      default:
        log.debug("LogicalValueType {} not supported, defaulting to keyword", valueType);
        return getMappingsForKeywordWithIgnoreAbove(keywordMaxLength);
    }
  }

  /**
   * Creates a mapping configuration for BROWSE_PATH_V2 fields. These fields use a special text
   * analyzer for hierarchy-based searching and include a length field for token counting.
   *
   * @return mapping configuration for BROWSE_PATH_V2 fields
   */
  @Nonnull
  private static Map<String, Object> getMappingsForBrowsePathV2() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", "text");
    mapping.put("analyzer", "browse_path_v2_hierarchy");
    mapping.put("fielddata", true);

    // Add fields for additional analysis
    Map<String, Object> fields = new HashMap<>();
    Map<String, Object> lengthField = new HashMap<>();
    lengthField.put("type", "token_count");
    lengthField.put("analyzer", "unit_separator_pattern");
    fields.put("length", lengthField);
    mapping.put("fields", fields);

    return mapping;
  }

  @Nonnull
  private static Map<String, Object> getMappingsForSearchText(
      @Nonnull FieldType fieldType, @Nonnull Map<String, String> partialNgramConfig) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    mapping.put(IGNORE_ABOVE, KEYWORD_MAXLENGTH);

    Map<String, Object> fields = new HashMap<>();
    if (fieldType == FieldType.TEXT_PARTIAL || fieldType == FieldType.WORD_GRAM) {
      fields.put(
          NGRAM,
          partialNgramConfigWithOverrides(partialNgramConfig, Map.of(ANALYZER, PARTIAL_ANALYZER)));
      if (fieldType == FieldType.WORD_GRAM) {
        fields.put(
            WORD_GRAMS_LENGTH_2,
            Map.of("type", ESUtils.TEXT_FIELD_TYPE, ANALYZER, WORD_GRAM_2_ANALYZER));
        fields.put(
            WORD_GRAMS_LENGTH_3,
            Map.of("type", ESUtils.TEXT_FIELD_TYPE, ANALYZER, WORD_GRAM_3_ANALYZER));
        fields.put(
            WORD_GRAMS_LENGTH_4,
            Map.of("type", ESUtils.TEXT_FIELD_TYPE, ANALYZER, WORD_GRAM_4_ANALYZER));
      }
    }
    fields.put(
        DELIMITED,
        Map.of(
            "type",
            ESUtils.TEXT_FIELD_TYPE,
            ANALYZER,
            TEXT_ANALYZER,
            SEARCH_ANALYZER,
            TEXT_SEARCH_ANALYZER,
            SEARCH_QUOTE_ANALYZER,
            CUSTOM_QUOTE_ANALYZER));
    fields.put(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_MAXLENGTH));
    mapping.put(FIELDS, fields);
    return mapping;
  }

  @Nonnull
  private static Map<String, Object> getMappingsForSearchableUrn(
      @Nonnull FieldType fieldType, @Nonnull Map<String, String> partialNgramConfig) {
    Map<String, Object> mapping = new HashMap<>();
    // The V2 shape: V2 and V3 share one query builder, which runs analyzed URN search on the field
    // itself. DataHub Cloud gives URN fields a keyword base with the analyzed text in a delimited
    // subfield, because its consolidated indices can map one root field name as KEYWORD for one
    // entity and URN for another; each OSS index holds a single entity, and a shared name falls
    // back to that shape (getMappingsForUrnSharedWithKeyword).
    mapping.put("type", ESUtils.TEXT_FIELD_TYPE);
    mapping.put(ANALYZER, URN_ANALYZER);
    mapping.put(SEARCH_ANALYZER, URN_SEARCH_ANALYZER);
    mapping.put(SEARCH_QUOTE_ANALYZER, CUSTOM_QUOTE_ANALYZER);

    Map<String, Object> fields = new HashMap<>();
    if (fieldType == FieldType.URN_PARTIAL) {
      fields.put(
          NGRAM,
          partialNgramConfigWithOverrides(
              partialNgramConfig, Map.of(ANALYZER, PARTIAL_URN_COMPONENT)));
    }
    fields.put(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE));
    mapping.put(FIELDS, fields);
    return mapping;
  }

  /**
   * DataHub Cloud's URN mapping, for a root field name that is a keyword or plain text field for
   * another entity of the index: a keyword base keeps exact match, sorting and aggregations working
   * for that entity, and analyzed URN search moves to the delimited subfield.
   */
  @Nonnull
  private static Map<String, Object> getMappingsForUrnSharedWithKeyword(
      @Nonnull FieldType fieldType, @Nonnull Map<String, String> partialNgramConfig) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(IGNORE_ABOVE, KEYWORD_MAXLENGTH);

    Map<String, Object> fields = new HashMap<>();
    if (fieldType == FieldType.URN_PARTIAL) {
      fields.put(
          NGRAM,
          partialNgramConfigWithOverrides(
              partialNgramConfig, Map.of(ANALYZER, PARTIAL_URN_COMPONENT)));
    }
    fields.put(
        DELIMITED,
        Map.of(
            "type",
            ESUtils.TEXT_FIELD_TYPE,
            ANALYZER,
            URN_ANALYZER,
            SEARCH_ANALYZER,
            URN_SEARCH_ANALYZER,
            SEARCH_QUOTE_ANALYZER,
            CUSTOM_QUOTE_ANALYZER));
    fields.put(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE));
    mapping.put(FIELDS, fields);
    return mapping;
  }

  @Nonnull
  private static Map<String, Object> partialNgramConfigWithOverrides(
      @Nonnull Map<String, String> partialNgramConfig, @Nonnull Map<String, String> overrides) {
    Map<String, Object> merged = new HashMap<>(partialNgramConfig);
    merged.putAll(overrides);
    return merged;
  }

  private static int mappingRichness(@Nonnull SearchableFieldSpec fieldSpec) {
    FieldType fieldType = fieldSpec.getSearchableAnnotation().getFieldType();
    switch (fieldType) {
      case WORD_GRAM:
        return 60;
      case TEXT_PARTIAL:
      case URN_PARTIAL:
        return 50;
      case TEXT:
      case URN:
        return 40;
      case KEYWORD:
        return 30;
      case BROWSE_PATH_V2:
        return 20;
      default:
        return 10;
    }
  }
}
