package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.BROWSE_PATH_HIERARCHY_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDDATA;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDS;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SLASH_PATTERN_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.LENGTH;
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
      case BROWSE_PATH:
      case BROWSE_PATH_V2:
        return "text"; // Browse path fields use text type with special analyzer
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
   * <p>Parent keyword only (no {@code .keyword} subfield): query-time structured-property filters
   * skip {@code .keyword} for URN value types via {@code
   * StructuredPropertyUtils.usesKeywordSubfield}. Root URN fields of entities are mapped like text
   * roots instead (see {@link #getMappingsForFieldType(FieldType)}).
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
    switch (fieldType) {
      case KEYWORD:
        return getMappingsForKeyword();
      case TEXT:
      case TEXT_PARTIAL:
      case WORD_GRAM:
      case URN:
      case URN_PARTIAL:
        return getMappingsForSearchText();
      case BOOLEAN:
        return Map.of("type", BOOLEAN_FIELD_TYPE);
      case COUNT:
        return Map.of("type", LONG_FIELD_TYPE);
      case DATETIME:
        return Map.of("type", DATE_FIELD_TYPE);
      case DOUBLE:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      case BROWSE_PATH:
        return getMappingsForBrowsePath();
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

    // For COUNT fields, try to determine the appropriate numeric type based on the underlying PDL
    // field type
    if (fieldType == FieldType.COUNT) {
      String elasticsearchType = getElasticsearchTypeForCountField(searchableFieldSpec);
      return Map.of("type", elasticsearchType);
    }

    // For all other field types, use the standard mapping
    return getMappingsForFieldType(fieldType);
  }

  /**
   * Creates the mapping for a field's copy under {@code _aspects.<aspect>}. Full-text search reads
   * the shared {@code _search} fields, so the aspect copy is never analyzed: string values map to
   * keywords guarded by {@code ignore_above}, and other types keep their standard mapping.
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
      @Nonnull List<SearchableFieldSpec> sourceFieldSpecs) {
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
    return getMappingsForFieldType(representativeType, representative);
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
   * Creates the V2 mapping for BROWSE_PATH fields: legacy browse aggregates on the path prefixes
   * and filters on the path depth ({@code length}).
   */
  @Nonnull
  private static Map<String, Object> getMappingsForBrowsePath() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", ESUtils.TEXT_FIELD_TYPE);
    mapping.put(ANALYZER, BROWSE_PATH_HIERARCHY_ANALYZER);
    mapping.put(FIELDDATA, true);
    mapping.put(
        FIELDS,
        Map.of(
            LENGTH,
            Map.of("type", ESUtils.TOKEN_COUNT_FIELD_TYPE, ANALYZER, SLASH_PATTERN_ANALYZER)));
    return mapping;
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

  /**
   * The root mapping of every text, word-gram and URN field: a normalized keyword, as V2 maps text
   * roots, with a {@code .keyword} subfield that keeps the stored casing for filters, facets and
   * sorts. Full-text search and autocomplete read the shared {@code _search} fields instead (see
   * {@link V3SearchFields}), so the root carries no analyzed subfields.
   */
  @Nonnull
  private static Map<String, Object> getMappingsForSearchText() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    mapping.put(IGNORE_ABOVE, KEYWORD_MAXLENGTH);
    mapping.put(
        FIELDS,
        Map.of(KEYWORD, Map.of("type", KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_MAXLENGTH)));
    return mapping;
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
