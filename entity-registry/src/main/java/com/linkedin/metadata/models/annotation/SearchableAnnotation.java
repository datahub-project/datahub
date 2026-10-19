package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

/** Simple object representation of the @Searchable annotation metadata. */
@Value
@AllArgsConstructor
public class SearchableAnnotation {

  public static final String FIELD_NAME_ALIASES = "fieldNameAliases";
  public static final String ANNOTATION_NAME = "Searchable";
  public static final Set<FieldType> OBJECT_FIELD_TYPES =
      ImmutableSet.of(FieldType.OBJECT, FieldType.MAP_ARRAY);
  private static final Set<FieldType> DEFAULT_QUERY_FIELD_TYPES =
      ImmutableSet.of(
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

  // Name of the field in the search index. Defaults to the field name in the schema
  String fieldName;
  // Type of the field. Defines how the field is indexed and matched
  FieldType fieldType;
  // Whether we should match the field for the default search query
  boolean queryByDefault;
  // Whether we should use the field for default autocomplete
  boolean enableAutocomplete;
  // Whether or not to add field to filters.
  boolean addToFilters;
  // Whether or not to add the "has values" to filters.
  boolean addHasValuesToFilters;
  // Display name of the filter
  Optional<String> filterNameOverride;
  // Display name of the has values filter
  Optional<String> hasValuesFilterNameOverride;
  // Boost multiplier to the match score. Matches on fields with higher boost score ranks higher
  double boostScore;
  // If set, add a index field of the given name that checks whether the field exists
  Optional<String> hasValuesFieldName;
  // If set, add a index field of the given name that checks the number of elements
  Optional<String> numValuesFieldName;
  // (Optional) Weights to apply to score for a given value
  Map<Object, Double> weightsPerFieldValue;
  // (Optional) Aliases for this given field that can be used for sorting etc.
  List<String> fieldNameAliases;
  // Whether to create a missing field aggregation when querying the corresponding field,
  // only adds to query time not mapping
  boolean includeQueryEmptyAggregation;

  boolean includeSystemModifiedAt;

  Optional<String> systemModifiedAtFieldName;

  // Shared Search V3 field. Parsed from v3.searchLabel; resolvedForV3 copies that value here.
  Optional<String> searchLabel;

  // Deprecated no-op: every searchable field is indexed under its own name
  Optional<Boolean> searchIndexed;

  // If set, this field is copied to the root field of this name and the root alias points there.
  // This allows multiple aspects to consolidate into a single entity-level field
  Optional<String> entityFieldName;

  // Whether to set eager_global_ordinals to true for this field
  // This improves aggregation performance for frequently aggregated keyword fields
  Optional<Boolean> eagerGlobalOrdinals;

  // Whether to sanitize rich text content (HTML, Markdown, base64 images) before indexing
  // This prevents indexing failures due to term size limits and improves search relevance
  boolean sanitizeRichText;

  // V3-only overrides. Absent means V3 uses the top-level annotation. V2 never reads these.
  boolean v3Searchable;

  Optional<String> v3SearchLabel;

  Optional<Boolean> v3IncludeSystemModifiedAt;

  /**
   * Search V3 root and label subfields. Explicit {@code v3.mappings} replace the fallback derived
   * from {@link #fieldType}. {@code _aspects} does not read this list.
   */
  @Nonnull List<FieldMapping> v3Mappings;

  /**
   * Constructor for annotations with no V3 overlay. V3 searches the field with the top-level
   * settings.
   */
  public SearchableAnnotation(
      String fieldName,
      FieldType fieldType,
      boolean queryByDefault,
      boolean enableAutocomplete,
      boolean addToFilters,
      boolean addHasValuesToFilters,
      Optional<String> filterNameOverride,
      Optional<String> hasValuesFilterNameOverride,
      double boostScore,
      Optional<String> hasValuesFieldName,
      Optional<String> numValuesFieldName,
      Map<Object, Double> weightsPerFieldValue,
      List<String> fieldNameAliases,
      boolean includeQueryEmptyAggregation,
      boolean includeSystemModifiedAt,
      Optional<String> systemModifiedAtFieldName,
      Optional<String> searchLabel,
      Optional<Boolean> searchIndexed,
      Optional<String> entityFieldName,
      Optional<Boolean> eagerGlobalOrdinals,
      boolean sanitizeRichText) {
    this(
        fieldName,
        fieldType,
        queryByDefault,
        enableAutocomplete,
        addToFilters,
        addHasValuesToFilters,
        filterNameOverride,
        hasValuesFilterNameOverride,
        boostScore,
        hasValuesFieldName,
        numValuesFieldName,
        weightsPerFieldValue,
        fieldNameAliases,
        includeQueryEmptyAggregation,
        includeSystemModifiedAt,
        systemModifiedAtFieldName,
        searchLabel,
        searchIndexed,
        entityFieldName,
        eagerGlobalOrdinals,
        sanitizeRichText,
        true,
        Optional.empty(),
        Optional.empty(),
        FieldMapping.fallback(fieldType, hasSearchDestination(searchLabel, entityFieldName)));
  }

  /**
   * The annotation Search V3 indexes. {@code searchLabel} is read only from the {@code v3} object.
   * The overlay can also turn the system-modified sidecar off or drop the field from V3 search.
   */
  @Nonnull
  public SearchableAnnotation resolvedForV3() {
    if (v3Searchable && v3SearchLabel.isEmpty() && v3IncludeSystemModifiedAt.isEmpty()) {
      return this;
    }
    final boolean includeModified = v3IncludeSystemModifiedAt.orElse(includeSystemModifiedAt);
    return new SearchableAnnotation(
        fieldName,
        fieldType,
        queryByDefault,
        enableAutocomplete,
        addToFilters,
        addHasValuesToFilters,
        filterNameOverride,
        hasValuesFilterNameOverride,
        boostScore,
        hasValuesFieldName,
        numValuesFieldName,
        weightsPerFieldValue,
        fieldNameAliases,
        includeQueryEmptyAggregation,
        includeModified,
        includeModified ? systemModifiedAtFieldName : Optional.empty(),
        v3SearchLabel,
        searchIndexed,
        entityFieldName,
        eagerGlobalOrdinals,
        sanitizeRichText,
        v3Searchable,
        Optional.empty(),
        Optional.empty(),
        v3Mappings);
  }

  public enum FieldType {
    KEYWORD,
    TEXT,
    TEXT_PARTIAL,
    BROWSE_PATH,
    URN,
    URN_PARTIAL,
    BOOLEAN,
    COUNT,
    DATETIME,
    OBJECT,
    BROWSE_PATH_V2,
    WORD_GRAM,
    DOUBLE,
    MAP_ARRAY
  }

  @Nonnull
  public static SearchableAnnotation fromPegasusAnnotationObject(
      @Nonnull final Object annotationObj,
      @Nonnull final String schemaFieldName,
      @Nonnull final DataSchema.Type schemaDataType,
      @Nonnull final String context) {
    if (!Map.class.isAssignableFrom(annotationObj.getClass())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid value type %s provided (Expected Map)",
              ANNOTATION_NAME, context, annotationObj.getClass()));
    }

    Map map = (Map) annotationObj;
    final Optional<String> fieldName = AnnotationUtils.getField(map, "fieldName", String.class);
    final Optional<String> fieldType = AnnotationUtils.getField(map, "fieldType", String.class);
    if (fieldType.isPresent() && !EnumUtils.isValidEnum(FieldType.class, fieldType.get())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: Invalid field 'fieldType'. Invalid fieldType provided. Valid types are %s",
              ANNOTATION_NAME, context, Arrays.toString(FieldType.values())));
    }

    final Optional<Boolean> queryByDefault =
        AnnotationUtils.getField(map, "queryByDefault", Boolean.class);
    final Optional<Boolean> enableAutocomplete =
        AnnotationUtils.getField(map, "enableAutocomplete", Boolean.class);
    final Optional<Boolean> addToFilters =
        AnnotationUtils.getField(map, "addToFilters", Boolean.class);
    final Optional<Boolean> addHasValuesToFilters =
        AnnotationUtils.getField(map, "addHasValuesToFilters", Boolean.class);
    final Optional<String> filterNameOverride =
        AnnotationUtils.getField(map, "filterNameOverride", String.class);
    final Optional<String> hasValuesFilterNameOverride =
        AnnotationUtils.getField(map, "hasValuesFilterNameOverride", String.class);
    final Optional<Double> boostScore = AnnotationUtils.getField(map, "boostScore", Double.class);
    final Optional<String> hasValuesFieldName =
        AnnotationUtils.getField(map, "hasValuesFieldName", String.class);
    final Optional<String> numValuesFieldName =
        AnnotationUtils.getField(map, "numValuesFieldName", String.class);
    final Optional<Map> weightsPerFieldValueMap =
        AnnotationUtils.getField(map, "weightsPerFieldValue", Map.class)
            .map(m -> (Map<Object, Double>) m);
    final Optional<Boolean> includeQueryEmptyAggregation =
        AnnotationUtils.getField(map, "includeQueryEmptyAggregation", Boolean.class);
    final List<String> fieldNameAliases = getFieldNameAliases(map);

    final FieldType resolvedFieldType = getFieldType(fieldType, schemaDataType);
    final Optional<Boolean> includeSystemModifiedAt =
        AnnotationUtils.getField(map, "includeSystemModifiedAt", Boolean.class);
    final Optional<String> systemModifiedAtFieldName =
        AnnotationUtils.getField(map, "systemModifiedAtFieldName", String.class);

    final Optional<Map> v3Overlay = AnnotationUtils.getField(map, "v3", Map.class);
    final boolean v3Searchable =
        v3Overlay
            .flatMap(overlay -> AnnotationUtils.getField(overlay, "enabled", Boolean.class))
            .orElse(resolvedFieldType != FieldType.BROWSE_PATH);
    if (resolvedFieldType == FieldType.BROWSE_PATH && v3Searchable) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: field type BROWSE_PATH is not"
                  + " supported on Search V3",
              ANNOTATION_NAME, context));
    }
    final Optional<String> v3SearchLabel =
        v3Overlay.flatMap(
            overlay -> AnnotationUtils.getField(overlay, "searchLabel", String.class));
    final Optional<Boolean> v3IncludeSystemModifiedAt =
        v3Overlay.flatMap(
            overlay -> AnnotationUtils.getField(overlay, "includeSystemModifiedAt", Boolean.class));
    final Optional<String> entityFieldName =
        AnnotationUtils.getField(map, "entityFieldName", String.class);
    final boolean hasSearchDestination = hasSearchDestination(v3SearchLabel, entityFieldName);
    final List<FieldMapping> v3Mappings =
        v3Searchable
            ? v3Overlay
                .flatMap(overlay -> AnnotationUtils.getField(overlay, "mappings", List.class))
                .map(raw -> FieldMapping.parse(raw, context))
                .orElseGet(() -> FieldMapping.fallback(resolvedFieldType, hasSearchDestination))
            : List.of();
    if (!hasSearchDestination && v3Mappings.stream().anyMatch(mapping -> !mapping.isRoot())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: a mappings subfield requires a"
                  + " searchLabel",
              ANNOTATION_NAME, context));
    }
    final Optional<Boolean> searchIndexed =
        AnnotationUtils.getField(map, "searchIndexed", Boolean.class);
    final Optional<Boolean> eagerGlobalOrdinals =
        AnnotationUtils.getField(map, "eagerGlobalOrdinals", Boolean.class);
    final Optional<Boolean> sanitizeRichText =
        AnnotationUtils.getField(map, "sanitizeRichText", Boolean.class);

    // Validate new fields
    validateSearchIndexed(searchIndexed, resolvedFieldType, context);
    validateElasticsearchFieldName(v3SearchLabel, "searchLabel", context);
    validateElasticsearchFieldName(entityFieldName, "entityFieldName", context);
    validateEagerGlobalOrdinals(eagerGlobalOrdinals, resolvedFieldType, context);
    validateSanitizeRichText(sanitizeRichText, resolvedFieldType, context);

    return new SearchableAnnotation(
        fieldName.orElse(schemaFieldName),
        resolvedFieldType,
        getQueryByDefault(queryByDefault, resolvedFieldType),
        enableAutocomplete.orElse(false),
        addToFilters.orElse(false),
        addHasValuesToFilters.orElse(false),
        filterNameOverride,
        hasValuesFilterNameOverride,
        boostScore.orElse(1.0),
        hasValuesFieldName,
        numValuesFieldName,
        weightsPerFieldValueMap.orElse(ImmutableMap.of()),
        fieldNameAliases,
        includeQueryEmptyAggregation.orElse(false),
        includeSystemModifiedAt.orElse(false),
        systemModifiedAtFieldName,
        Optional.empty(),
        searchIndexed,
        entityFieldName,
        eagerGlobalOrdinals,
        sanitizeRichText.orElse(false),
        v3Searchable,
        v3SearchLabel,
        v3IncludeSystemModifiedAt,
        v3Mappings);
  }

  private static boolean hasSearchDestination(
      Optional<String> searchLabel, Optional<String> entityFieldName) {
    return searchLabel.filter(label -> !label.isEmpty()).isPresent()
        || entityFieldName.filter(name -> !name.isEmpty()).isPresent();
  }

  private static FieldType getFieldType(
      Optional<String> maybeFieldType, DataSchema.Type schemaDataType) {
    if (!maybeFieldType.isPresent()) {
      return getDefaultFieldType(schemaDataType);
    }
    return FieldType.valueOf(maybeFieldType.get());
  }

  private static FieldType getDefaultFieldType(DataSchema.Type schemaDataType) {
    switch (schemaDataType) {
      case INT:
        return FieldType.COUNT;
      case MAP:
        return FieldType.KEYWORD;
      case FLOAT:
      case DOUBLE:
        return FieldType.DOUBLE;
      default:
        return FieldType.TEXT;
    }
  }

  private static Boolean getQueryByDefault(
      Optional<Boolean> maybeQueryByDefault, FieldType fieldType) {
    if (!maybeQueryByDefault.isPresent()) {
      if (DEFAULT_QUERY_FIELD_TYPES.contains(fieldType)) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    return maybeQueryByDefault.get();
  }

  public String getFilterName() {
    return filterNameOverride.orElse(fieldName);
  }

  public String getHasValuesFilterName() {
    return hasValuesFilterNameOverride.orElse(
        hasValuesFieldName.orElse(String.format("has%s", capitalizeFirstLetter(fieldName))));
  }

  private static String capitalizeFirstLetter(String str) {
    if (str == null || str.length() == 0) {
      return str;
    } else {
      return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
  }

  private static List<String> getFieldNameAliases(Map map) {
    final List<String> aliases = new ArrayList<>();
    final Optional<List> fieldNameAliases =
        AnnotationUtils.getField(map, FIELD_NAME_ALIASES, List.class);
    if (fieldNameAliases.isPresent()) {
      for (Object alias : fieldNameAliases.get()) {
        aliases.add((String) alias);
      }
    }
    return aliases;
  }

  /** Validates that searchIndexed is only used with KEYWORD or TEXT fields. */
  private static void validateSearchIndexed(
      Optional<Boolean> searchIndexed, FieldType fieldType, String context) {
    if (searchIndexed.isPresent() && searchIndexed.get()) {
      if (fieldType != FieldType.KEYWORD && fieldType != FieldType.TEXT) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchIndexed can only be used with KEYWORD or TEXT field types, but was %s",
                ANNOTATION_NAME, context, fieldType));
      }
    }
  }

  /**
   * Validates that field names are valid Elasticsearch field names (no spaces, valid characters)
   */
  private static void validateElasticsearchFieldName(
      Optional<String> fieldName, String fieldType, String context) {
    if (fieldName.isPresent()) {
      String name = fieldName.get();
      if (name.trim().isEmpty()) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: %s cannot be empty",
                ANNOTATION_NAME, context, fieldType));
      }

      // Check for spaces
      if (name.contains(" ")) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: %s cannot contain spaces, but was '%s'",
                ANNOTATION_NAME, context, fieldType, name));
      }

      // Check for invalid characters (Elasticsearch field names should not start with _, contain
      // certain special chars)
      if (name.startsWith("_")) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: %s cannot start with underscore, but was '%s'",
                ANNOTATION_NAME, context, fieldType, name));
      }

      // Check for other invalid characters for Elasticsearch field names
      if (!name.matches("^[a-zA-Z][a-zA-Z0-9._-]*$")) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: %s contains invalid characters. Must start with a letter and contain only letters, numbers, dots, hyphens, and underscores, but was '%s'",
                ANNOTATION_NAME, context, fieldType, name));
      }

      // Additional validation specific to entityFieldName
      if ("entityFieldName".equals(fieldType)) {
        validateEntityFieldName(name, context);
      }
    }
  }

  /** Validates that entityFieldName is suitable for Elasticsearch and follows best practices */
  private static void validateEntityFieldName(String entityFieldName, String context) {
    // Check length - Elasticsearch has a limit on field name length
    if (entityFieldName.length() > 255) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: entityFieldName cannot exceed 255 characters, but was %d characters long",
              ANNOTATION_NAME, context, entityFieldName.length()));
    }

    // Check for reserved Elasticsearch field names
    if (isReservedElasticsearchField(entityFieldName)) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: entityFieldName cannot use reserved Elasticsearch field name '%s'",
              ANNOTATION_NAME, context, entityFieldName));
    }

    // Check for common problematic patterns
    if (entityFieldName.contains("..")
        || entityFieldName.endsWith(".")
        || entityFieldName.startsWith(".")) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: entityFieldName cannot start or end with dots or contain consecutive dots, but was '%s'",
              ANNOTATION_NAME, context, entityFieldName));
    }

    // Check for uppercase letters (Elasticsearch field names are case-sensitive, but lowercase is
    // recommended)
    if (!entityFieldName.equals(entityFieldName.toLowerCase())) {
      throw new ModelValidationException(
          String.format(
              "Failed to validate @%s annotation declared at %s: entityFieldName should use lowercase letters for consistency, but was '%s'",
              ANNOTATION_NAME, context, entityFieldName));
    }
  }

  /** Checks if a field name is a reserved Elasticsearch field name */
  private static boolean isReservedElasticsearchField(String fieldName) {
    // Common reserved Elasticsearch field names
    Set<String> reservedFields =
        ImmutableSet.of(
            "_id",
            "_index",
            "_type",
            "_source",
            "_all",
            "_routing",
            "_parent",
            "_timestamp",
            "_ttl",
            "_version",
            "_score",
            "_explanation",
            "_shards",
            "_nodes",
            "_cluster_name",
            "_cluster_uuid",
            "_name",
            "_uuid",
            "_version_type",
            "_seq_no",
            "_primary_term",
            "_version_type",
            "_seq_no",
            "_primary_term",
            "_version_type",
            "_seq_no",
            "_primary_term");

    return reservedFields.contains(fieldName.toLowerCase());
  }

  /** Validates that eagerGlobalOrdinals is only used with appropriate field types */
  private static void validateEagerGlobalOrdinals(
      Optional<Boolean> eagerGlobalOrdinals, FieldType fieldType, String context) {
    if (eagerGlobalOrdinals.isPresent() && eagerGlobalOrdinals.get()) {
      // eagerGlobalOrdinals can only be true for keyword fields
      if (fieldType != FieldType.KEYWORD
          && fieldType != FieldType.URN
          && fieldType != FieldType.URN_PARTIAL) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: eagerGlobalOrdinals can only be true for KEYWORD, URN, or URN_PARTIAL field types, but was %s",
                ANNOTATION_NAME, context, fieldType));
      }
    }
  }

  /** Validates that sanitizeRichText is only used with TEXT or TEXT_PARTIAL field types */
  private static void validateSanitizeRichText(
      Optional<Boolean> sanitizeRichText, FieldType fieldType, String context) {
    if (sanitizeRichText.isPresent() && sanitizeRichText.get()) {
      if (fieldType != FieldType.TEXT && fieldType != FieldType.TEXT_PARTIAL) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: sanitizeRichText can only be used with TEXT or TEXT_PARTIAL field types, but was %s",
                ANNOTATION_NAME, context, fieldType));
      }
    }
  }
}
