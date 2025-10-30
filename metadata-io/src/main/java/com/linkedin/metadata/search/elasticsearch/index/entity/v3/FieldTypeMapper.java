package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.utils.ESUtils.BOOLEAN_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DATE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.DOUBLE_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.FLOAT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.INTEGER_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.LONG_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.OBJECT_FIELD_TYPE;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for mapping DataHub field types to Elasticsearch field types and configurations.
 * This class centralizes all field type mapping logic to improve maintainability.
 */
@Slf4j
public class FieldTypeMapper {

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
    return Map.of("type", KEYWORD_FIELD_TYPE);
  }

  /**
   * Creates a mapping configuration for a URN field.
   *
   * @return mapping configuration for URN field
   */
  @Nonnull
  public static Map<String, Object> getMappingsForUrn() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("type", KEYWORD_FIELD_TYPE);
    mapping.put("ignore_above", 255);
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
        return getMappingsForKeyword(); // Treat text fields as keyword for simplicity
      case BOOLEAN:
        return Map.of("type", BOOLEAN_FIELD_TYPE);
      case COUNT:
        return Map.of("type", LONG_FIELD_TYPE);
      case DATETIME:
        return Map.of("type", DATE_FIELD_TYPE);
      case DOUBLE:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      case URN:
        return getMappingsForUrn();
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
    switch (valueType) {
      case STRING:
      case RICH_TEXT:
        return getMappingsForKeyword();
      case DATE:
        return Map.of("type", DATE_FIELD_TYPE);
      case URN:
        return getMappingsForUrn();
      case NUMBER:
        return Map.of("type", DOUBLE_FIELD_TYPE);
      default:
        log.debug("LogicalValueType {} not supported, defaulting to keyword", valueType);
        return getMappingsForKeyword();
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
}
