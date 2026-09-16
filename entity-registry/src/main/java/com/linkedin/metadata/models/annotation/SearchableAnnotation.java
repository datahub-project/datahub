package com.linkedin.metadata.models.annotation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.ModelValidationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.commons.lang3.EnumUtils;

/** Simple object representation of the @Searchable annotation metadata. */
@Value
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

  // Search tier for the field (integer value)
  Optional<Integer> searchTier;

  // Unified label for search operations
  Optional<String> searchLabel;

  // Whether to index the field as a KEYWORD outside of _search when searchTier is present
  Optional<Boolean> searchIndexed;

  // If set, this field will be copied to _search.<entityFieldName> and the root alias will point
  // there
  // This allows multiple aspects to consolidate into a single entity-level field
  Optional<String> entityFieldName;

  // Whether to set eager_global_ordinals to true for this field
  // This improves aggregation performance for frequently aggregated keyword fields
  Optional<Boolean> eagerGlobalOrdinals;

  // Whether to sanitize rich text content (HTML, Markdown, base64 images) before indexing
  // This prevents indexing failures due to term size limits and improves search relevance
  boolean sanitizeRichText;

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

    // Parse new fields
    final Optional<Integer> searchTier = AnnotationUtils.getField(map, "searchTier", Integer.class);
    final Optional<String> searchLabel = AnnotationUtils.getField(map, "searchLabel", String.class);
    final Optional<Boolean> searchIndexed =
        AnnotationUtils.getField(map, "searchIndexed", Boolean.class);
    final Optional<String> entityFieldName =
        AnnotationUtils.getField(map, "entityFieldName", String.class);
    final Optional<Boolean> eagerGlobalOrdinals =
        AnnotationUtils.getField(map, "eagerGlobalOrdinals", Boolean.class);
    final Optional<Boolean> sanitizeRichText =
        AnnotationUtils.getField(map, "sanitizeRichText", Boolean.class);

    // Validate new fields
    validateSearchTier(searchTier, resolvedFieldType, context);
    validateSearchIndexed(searchIndexed, resolvedFieldType, searchTier, context);
    validateElasticsearchFieldName(searchLabel, "searchLabel", context);
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
        searchTier,
        searchLabel,
        searchIndexed,
        entityFieldName,
        eagerGlobalOrdinals,
        sanitizeRichText.orElse(false));
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

  /**
   * Validates that searchTier is >= 1 if present and that fields with searchTier are of type
   * KEYWORD, TEXT, TEXT_PARTIAL, WORD_GRAM, or URN
   */
  private static void validateSearchTier(
      Optional<Integer> searchTier, FieldType fieldType, String context) {
    if (searchTier.isPresent()) {
      // Validate searchTier value is >= 1
      if (searchTier.get() < 1) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchTier must be >= 1, but was %d",
                ANNOTATION_NAME, context, searchTier.get()));
      }

      // Validate that fields with searchTier are of type KEYWORD, TEXT, TEXT_PARTIAL, WORD_GRAM, or
      // URN
      if (fieldType != FieldType.KEYWORD
          && fieldType != FieldType.TEXT
          && fieldType != FieldType.TEXT_PARTIAL
          && fieldType != FieldType.WORD_GRAM
          && fieldType != FieldType.URN) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchTier can only be used with KEYWORD, TEXT, TEXT_PARTIAL, WORD_GRAM, or URN field types, but was %s",
                ANNOTATION_NAME, context, fieldType));
      }
    }
  }

  /** Validates that searchIndexed is only used with appropriate field types and searchTier */
  private static void validateSearchIndexed(
      Optional<Boolean> searchIndexed,
      FieldType fieldType,
      Optional<Integer> searchTier,
      String context) {
    if (searchIndexed.isPresent() && searchIndexed.get()) {
      // searchIndexed can only be true when searchTier is present
      if (!searchTier.isPresent()) {
        throw new ModelValidationException(
            String.format(
                "Failed to validate @%s annotation declared at %s: searchIndexed can only be true when searchTier is specified",
                ANNOTATION_NAME, context));
      }

      // searchIndexed can only be used with KEYWORD or TEXT field types
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
