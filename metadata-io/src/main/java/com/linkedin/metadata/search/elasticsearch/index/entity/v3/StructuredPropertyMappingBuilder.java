package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.models.StructuredPropertyUtils.entityTypeMatches;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
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
                  Map<String, Object> fieldMapping =
                      getMappingsForStructuredProperty(definition, List.of(entitySpec));
                  return new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                      fieldName, propertyUrn, fieldMapping);
                })
            .collect(Collectors.toList());

    Map<String, Object> resolved =
        StructuredPropertyUtils.resolveStructuredPropertyMappingCollisions(entries);
    // The identity field follows the value field that survived collision resolution.
    structuredProperties.stream()
        .filter(
            pair -> {
              StructuredPropertyDefinition definition = pair.getValue();
              return definition.getEntityTypes() != null
                  && definition.getEntityTypes().stream()
                      .anyMatch(entityTypeUrn -> entityTypeMatches(entityTypeUrn, entityType))
                  && StructuredPropertyUrnIdentity.applies(
                      getLogicalValueType(definition.getValueType()));
            })
        .forEach(
            pair -> {
              String fieldName = toElasticsearchFieldName(pair.getKey(), pair.getValue());
              if (resolved.containsKey(fieldName)) {
                resolved.put(
                    StructuredPropertyUrnIdentity.fieldName(fieldName),
                    StructuredPropertyUrnIdentity.mapping(pair.getValue(), List.of(entitySpec)));
              }
            });
    return resolved;
  }

  /**
   * Creates mappings for a single structured property.
   *
   * @param definition the structured property definition
   * @return map of field mappings for the structured property
   */
  public static Map<String, Object> getMappingsForStructuredProperty(
      @Nonnull StructuredPropertyDefinition definition) {
    return getMappingsForStructuredProperty(definition, List.of());
  }

  /**
   * Creates mappings for a single structured property. {@code entitySpecs} resolve {@code copyTo}
   * to the shared fields those entities search.
   */
  public static Map<String, Object> getMappingsForStructuredProperty(
      @Nonnull StructuredPropertyDefinition definition,
      @Nonnull Collection<EntitySpec> entitySpecs) {

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

    // Presence of searchConfiguration opts a string or rich text property into full-text subfields
    // on its root keyword. A URN property keeps that keyword exact, adds .lowercase, and puts the
    // parsed name on a sibling field. queryByDefault only decides whether a default query reads
    // the analyzed fields, and excludeFromFullTextSearch is a V2 control V3 ignores. copyTo copies
    // the parsed name. copyUrnTo copies the complete URN. Two properties whose names collide and
    // disagree on that presence, or on either copy list, get different mappings, so the collision
    // resolver omits the field.
    if (logicalValueType == LogicalValueType.URN) {
      Map<String, Object> fields = new HashMap<>();
      Object existingFields = fieldMapping.get(FIELDS);
      if (existingFields instanceof Map<?, ?> existing) {
        existing.forEach((key, value) -> fields.put(String.valueOf(key), value));
      }
      fields.put(V3SearchFields.LOWERCASE, V3SearchFields.lowercaseKeywordField());
      fieldMapping.put(FIELDS, fields);
      List<String> copyUrnTo =
          V3SearchFields.copyDestinations(entitySpecs, copyUrnToNames(definition)).stream()
              .map(V3SearchFields::path)
              .collect(Collectors.toList());
      if (!copyUrnTo.isEmpty()) {
        fieldMapping.put(COPY_TO, copyUrnTo);
      }
    }
    if (isFullTextEligible(logicalValueType)
        && definition.hasSearchConfiguration()
        && logicalValueType != LogicalValueType.URN) {
      Map<String, Object> fields = new HashMap<>();
      Object existingFields = fieldMapping.get(FIELDS);
      if (existingFields instanceof Map<?, ?> existing) {
        existing.forEach((key, value) -> fields.put(String.valueOf(key), value));
      }
      fields.putAll(V3SearchFields.analyzedSubfields());
      fieldMapping.put(FIELDS, fields);
      List<String> copyTo =
          V3SearchFields.copyDestinations(entitySpecs, copyToNames(definition)).stream()
              .map(V3SearchFields::path)
              .collect(Collectors.toList());
      if (!copyTo.isEmpty()) {
        fieldMapping.put(COPY_TO, copyTo);
      }
    }

    return fieldMapping;
  }

  @Nonnull
  private static List<String> copyToNames(@Nonnull StructuredPropertyDefinition definition) {
    if (!definition.hasSearchConfiguration() || !definition.getSearchConfiguration().hasCopyTo()) {
      return List.of();
    }
    return definition.getSearchConfiguration().getCopyTo().stream()
        .filter(name -> name != null && !name.isBlank())
        .collect(Collectors.toList());
  }

  @Nonnull
  private static List<String> copyUrnToNames(@Nonnull StructuredPropertyDefinition definition) {
    if (!definition.hasSearchConfiguration()
        || !definition.getSearchConfiguration().hasCopyUrnTo()) {
      return List.of();
    }
    return definition.getSearchConfiguration().getCopyUrnTo().stream()
        .filter(name -> name != null && !name.isBlank())
        .collect(Collectors.toList());
  }

  private static boolean isFullTextEligible(@Nonnull LogicalValueType logicalType) {
    return logicalType == LogicalValueType.STRING
        || logicalType == LogicalValueType.RICH_TEXT
        || logicalType == LogicalValueType.URN;
  }
}
