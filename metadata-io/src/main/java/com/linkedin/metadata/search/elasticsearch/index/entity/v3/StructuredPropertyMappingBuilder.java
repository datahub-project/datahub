package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.StructuredPropertyUtils.entityTypeMatches;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Builder class for creating structured property mappings. Handles the creation of mappings for
 * structured properties at the root level.
 */
public class StructuredPropertyMappingBuilder {

  /**
   * Creates mappings for structured properties.
   *
   * @param entitySpec the entity spec to process
   * @param structuredProperties collection of structured property definitions
   * @return map of structured property mappings
   */
  public static Map<String, Object> createStructuredPropertyMappings(
      @Nonnull EntitySpec entitySpec,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {

    Map<String, Object> mappings = new HashMap<>();

    if (structuredProperties == null || structuredProperties.isEmpty()) {
      return mappings;
    }

    // Filter structured properties for this entity type
    String entityType = entitySpec.getEntityAnnotation().getName();
    structuredProperties.stream()
        .filter(
            pair -> {
              StructuredPropertyDefinition definition = pair.getValue();
              return definition.getEntityTypes() != null
                  && definition.getEntityTypes().stream()
                      .anyMatch(entityTypeUrn -> entityTypeMatches(entityTypeUrn, entityType));
            })
        .forEach(
            pair -> {
              Urn propertyUrn = pair.getKey();
              StructuredPropertyDefinition definition = pair.getValue();
              String fieldName = toElasticsearchFieldName(propertyUrn, definition);
              Map<String, Object> fieldMapping = getMappingsForStructuredProperty(definition);
              mappings.put(fieldName, fieldMapping);
            });

    return mappings;
  }

  /**
   * Creates mappings for a single structured property.
   *
   * @param definition the structured property definition
   * @return map of field mappings for the structured property
   */
  public static Map<String, Object> getMappingsForStructuredProperty(
      @Nonnull StructuredPropertyDefinition definition) {

    Map<String, Object> fieldMapping = new HashMap<>();

    LogicalValueType logicalValueType = getLogicalValueType(definition.getValueType());
    String elasticsearchType =
        FieldTypeMapper.getElasticsearchTypeForLogicalValueType(logicalValueType);

    fieldMapping.put(TYPE, elasticsearchType);

    // Add specific properties based on the value type
    Map<String, Object> properties =
        FieldTypeMapper.getMappingsForLogicalValueType(logicalValueType);
    if (!properties.isEmpty()) {
      fieldMapping.putAll(properties);
    }

    return fieldMapping;
  }
}
