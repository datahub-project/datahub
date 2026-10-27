package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.BROWSE_PATH_HIERARCHY_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.FIELDDATA;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.PARTIAL_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SLASH_PATTERN_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.WORD_GRAM_2_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.LENGTH;
import static com.linkedin.metadata.search.utils.ESUtils.BOOLEAN_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DATE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DOUBLE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
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
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.search.elasticsearch.client.shim.impl.OpenSearchSearchClientShim;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
   * Exact URN keyword with no {@code .keyword} subfield. Query-time structured-property filters
   * skip {@code .keyword} for URN value types. The whole value is indexed, including dataset and
   * schema-field paths longer than 255 bytes. The parsed name is the aspect keyword {@code
   * <field>__urn_name}.
   */
  @Nonnull
  public static Map<String, Object> getMappingsForUrn() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
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
   * Creates the mapping for a field's copy under {@code _aspects.<aspect>}. This is the filter
   * surface for every searchable aspect field, labeled or not. It does not read {@code
   * v3.mappings}: a search-only label root and analyzer subfields stay on the shared label. String
   * values map to keywords guarded by a byte-safe {@code ignore_above}, and dates, numbers and
   * booleans keep their native type. Nothing here is analyzed text.
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

  /**
   * Maps the exact representation under {@code _aspects}. A field also projected to the document
   * root leaves this copy unindexed unless {@code v3.source} turns index or doc values back on. A
   * field that stays under {@code _aspects} follows the V3 root. Label analyzer subfields never
   * appear under an aspect.
   */
  @Nonnull
  public static Map<String, Object> mappingForAspectRoot(
      @Nonnull FieldMapping root, @Nonnull SearchableFieldSpec searchableFieldSpec) {
    if (!root.isRoot()) {
      throw new IllegalArgumentException("mappingForAspectRoot requires the root entry");
    }
    SearchableAnnotation resolved = searchableFieldSpec.getSearchableAnnotation().resolvedForV3();
    boolean index = resolved.aspectIndexed();
    boolean docValues = resolved.aspectDocValues();
    FieldType fieldType = resolved.getFieldType();
    switch (root.getType()) {
      case KEYWORD:
        if (fieldType == FieldType.URN || fieldType == FieldType.URN_PARTIAL) {
          return urnAspectMapping(index, docValues);
        }
        return keywordRoot(FieldMapping.root(FieldMapping.Type.KEYWORD, index, docValues));
      case URN:
        return urnAspectMapping(index, docValues);
      case BOOLEAN:
        return nativeAspectMapping(BOOLEAN_FIELD_TYPE, index, docValues);
      case COUNT:
        return nativeAspectMapping(
            getElasticsearchTypeForCountField(searchableFieldSpec), index, docValues);
      case DATETIME:
        return nativeAspectMapping(DATE_FIELD_TYPE, index, docValues);
      case DOUBLE:
        return nativeAspectMapping(DOUBLE_FIELD_TYPE, index, docValues);
      case BROWSE_PATH_V2:
        if (!index) {
          return unindexedKeyword(docValues);
        }
        Map<String, Object> browse = new HashMap<>(getMappingsForKeywordWithIgnoreAbove());
        if (!docValues) {
          browse.put("doc_values", false);
        }
        return browse;
      default:
        return mappingForRoot(
            FieldMapping.root(root.getType(), index, docValues), searchableFieldSpec);
    }
  }

  /** Exact URN keyword under {@code _aspects}. Unindexed copies keep {@code copy_to}. */
  @Nonnull
  private static Map<String, Object> urnAspectMapping(boolean index, boolean docValues) {
    if (!index) {
      return unindexedKeyword(docValues);
    }
    Map<String, Object> mapping = new HashMap<>(getMappingsForUrn());
    if (!docValues) {
      mapping.put("doc_values", false);
    }
    return mapping;
  }

  /**
   * A native aspect value. Both flags true omit the keys so an unlabeled field matches the mapping
   * it had before aspect-source defaults.
   */
  @Nonnull
  private static Map<String, Object> nativeAspectMapping(
      @Nonnull String type, boolean index, boolean docValues) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", type);
    if (!index) {
      mapping.put(ESUtils.INDEX, false);
    }
    if (!docValues) {
      mapping.put("doc_values", false);
    }
    return mapping;
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
   * sorts. Full-text search and autocomplete read the shared root fields (see {@link
   * V3SearchFields}), whose analyzed subfields are merged onto this projection.
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

  /**
   * The document-root field: the annotation's exact root only. Analyzer subfields are not indexed
   * here; {@code copy_to} feeds the shared label.
   */
  @Nonnull
  public static Map<String, Object> mappingForRoot(
      @Nonnull FieldMapping root, @Nullable SearchableFieldSpec searchableFieldSpec) {
    if (!root.isRoot()) {
      throw new IllegalArgumentException("mappingForRoot requires the root entry");
    }
    switch (root.getType()) {
      case KEYWORD:
        return keywordRoot(root);
      case URN:
        return root.isIndex()
            ? new HashMap<>(getMappingsForUrn())
            : unindexedKeyword(root.isDocValues());
      case BOOLEAN:
        return Map.of("type", BOOLEAN_FIELD_TYPE);
      case COUNT:
        return Map.of(
            "type",
            searchableFieldSpec == null
                ? LONG_FIELD_TYPE
                : getElasticsearchTypeForCountField(searchableFieldSpec));
      case DATETIME:
        return Map.of("type", DATE_FIELD_TYPE);
      case DOUBLE:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      case BROWSE_PATH_V2:
        return getMappingsForBrowsePathV2();
      case OBJECT:
        return getDynamicObjectMappings();
      case TEXT:
      case STEMMED:
      case TEXT_PARTIAL:
      case WORD_GRAM_2:
      default:
        throw new IllegalArgumentException("Analyzer type " + root.getType() + " cannot be a root");
    }
  }

  /**
   * A shared label: one exact root plus the alternate representations its contributors declared.
   * Analyzed subfields have no {@code doc_values}. A long value skipped by keyword {@code
   * ignore_above} is still available to those subfields.
   */
  @Nonnull
  public static Map<String, Object> composeLabelMapping(
      @Nonnull FieldMapping.Resolved resolved,
      @Nullable SearchableFieldSpec searchableFieldSpec,
      @Nonnull Map<String, String> partialNgramConfig) {
    Map<String, Object> mapping =
        new HashMap<>(mappingForRoot(resolved.getRoot(), searchableFieldSpec));
    if (resolved.getSubfields().isEmpty()) {
      return mapping;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> subfields =
        mapping.get(FIELDS) instanceof Map
            ? new HashMap<>((Map<String, Object>) mapping.get(FIELDS))
            : new HashMap<>();
    for (FieldMapping subfield : resolved.getSubfields()) {
      subfields.put(subfield.getSubfield(), mappingForSubfield(subfield, partialNgramConfig));
    }
    mapping.put(FIELDS, subfields);
    return mapping;
  }

  @Nonnull
  public static Map<String, Object> mappingForSubfield(
      @Nonnull FieldMapping subfield, @Nonnull Map<String, String> partialNgramConfig) {
    switch (subfield.getType()) {
      case TEXT:
        return Map.of(
            "type",
            ESUtils.TEXT_FIELD_TYPE,
            ANALYZER,
            V3SearchFields.TEXT_ANALYZER,
            SEARCH_ANALYZER,
            V3SearchFields.TEXT_SEARCH_ANALYZER);
      case STEMMED:
        return Map.of(
            "type",
            ESUtils.TEXT_FIELD_TYPE,
            ANALYZER,
            V3SearchFields.STEMMED_ANALYZER,
            SEARCH_ANALYZER,
            V3SearchFields.STEMMED_SEARCH_ANALYZER);
      case TEXT_PARTIAL:
        Map<String, Object> partial = new HashMap<>(partialNgramConfig);
        partial.put(ANALYZER, PARTIAL_ANALYZER);
        return partial;
      case WORD_GRAM_2:
        return Map.of("type", ESUtils.TEXT_FIELD_TYPE, ANALYZER, WORD_GRAM_2_ANALYZER);
      case KEYWORD:
      case URN:
        int ignoreAbove = keywordIgnoreAboveForMaxBytes(KEYWORD_MAXLENGTH);
        return Map.of("type", KEYWORD_FIELD_TYPE, IGNORE_ABOVE, ignoreAbove);
      default:
        throw new IllegalArgumentException(subfield.getType() + " is not a subfield type");
    }
  }

  @Nonnull
  private static Map<String, Object> keywordRoot(@Nonnull FieldMapping root) {
    if (!root.isIndex()) {
      return unindexedKeyword(root.isDocValues());
    }
    Map<String, Object> mapping = new HashMap<>(getMappingsForKeywordWithIgnoreAbove());
    if (!root.isDocValues()) {
      mapping.put("doc_values", false);
    }
    return mapping;
  }

  @Nonnull
  private static Map<String, Object> unindexedKeyword(boolean docValues) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put(ESUtils.INDEX, false);
    mapping.put("doc_values", docValues);
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
