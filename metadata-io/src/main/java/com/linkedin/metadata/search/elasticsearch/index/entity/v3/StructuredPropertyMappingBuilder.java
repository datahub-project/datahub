package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.StructuredPropertyUtils.entityTypeMatches;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

    if (structuredProperties == null || structuredProperties.isEmpty()) {
      return Map.of();
    }

    // Filter structured properties for this entity type
    String entityType = entitySpec.getEntityAnnotation().getName();
    List<StructuredPropertyUtils.StructuredPropertyFieldMapping> entries =
        structuredProperties.stream()
            .filter(
                pair -> {
                  StructuredPropertyDefinition definition = pair.getValue();
                  return definition.getEntityTypes() != null
                      && definition.getEntityTypes().stream()
                          .anyMatch(entityTypeUrn -> entityTypeMatches(entityTypeUrn, entityType));
                })
            .map(
                pair -> {
                  Urn propertyUrn = pair.getKey();
                  StructuredPropertyDefinition definition = pair.getValue();
                  String fieldName = toElasticsearchFieldName(propertyUrn, definition);
                  Map<String, Object> fieldMapping = getMappingsForStructuredProperty(definition);
                  return new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                      fieldName, propertyUrn, fieldMapping);
                })
            .collect(Collectors.toList());

    return StructuredPropertyUtils.resolveStructuredPropertyMappingCollisions(entries);
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

    if (isFullTextEligible(logicalValueType) && !isExcludedFromFullTextSearch(definition)) {
      fieldMapping.put(COPY_TO, List.of(V3SearchFields.path(V3SearchFields.STRUCTURED_PROPERTIES)));
    }

    return fieldMapping;
  }

  private static boolean isFullTextEligible(@Nonnull LogicalValueType logicalType) {
    return logicalType == LogicalValueType.STRING
        || logicalType == LogicalValueType.RICH_TEXT
        || logicalType == LogicalValueType.URN;
  }

  /**
   * Per-property opt-out from the full-text field. Read only when the property's mapping is built,
   * and copy_to applies at index time, so changing it on a property already in an index takes
   * effect once that index is rebuilt from the property definitions: system-update reindexes an
   * index whose structured property copy_to differs when structured property system update and
   * mappings reindex are both on. Two properties whose names collide on one field (rejected by
   * PropertyDefinitionValidator) and disagree on it get different mappings, so the collision
   * resolver omits the field, as for any other divergent mapping.
   */
  private static boolean isExcludedFromFullTextSearch(
      @Nonnull StructuredPropertyDefinition definition) {
    return definition.hasSearchConfiguration()
        && Boolean.TRUE.equals(definition.getSearchConfiguration().isExcludeFromFullTextSearch());
  }
}
