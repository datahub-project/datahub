package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getEntityTypeId;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.utils.ESUtils.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.INDEX;
import static com.linkedin.metadata.search.utils.ESUtils.PATH;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.FieldSpecUtils;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Builder for Elasticsearch mappings that supports multiple entities in a unified index structure.
 * This class implements the v3 search architecture where entities are grouped by search groups and
 * share common index mappings.
 *
 * <p>The MultiEntityMappingsBuilder creates mappings with the following structure:
 *
 * <ul>
 *   <li>Aspect-based field organization under {@code _aspects} object
 *   <li>Root-level aliases for single-aspect fields
 *   <li>Root-level fields with copy_to for multi-aspect conflicts
 *   <li>Structured properties support under {@code structuredProperties} field
 *   <li>Search tier and label organization under {@code _search} object
 * </ul>
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Conflict resolution for fields appearing in multiple aspects
 *   <li>Type conflict resolution using configurable strategies
 *   <li>Support for field name aliases
 *   <li>Dynamic structured properties handling
 *   <li>Search tier and label organization
 *   <li>Eager global ordinals optimization
 * </ul>
 *
 * <p>Example mapping structure:
 *
 * <pre>{@code
 * {
 *   "properties": {
 *     "_aspects": {
 *       "properties": {
 *         "ownership": {
 *           "properties": {
 *             "owners": { "type": "keyword", "copy_to": "owners" }
 *           }
 *         }
 *       }
 *     },
 *     "owners": { "type": "keyword" },
 *     "_search": {
 *       "properties": {
 *         "tier_1": { "type": "keyword" },
 *         "entityName": { "type": "keyword" }
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * @see MappingsBuilder
 * @see EntityIndexConfiguration
 * @see ConflictResolver
 * @see AspectMappingBuilder
 * @see StructuredPropertyMappingBuilder
 */
@Slf4j
public class MultiEntityMappingsBuilder implements MappingsBuilder {

  /** Configuration for entity indexing behavior and v3 search settings. */
  private final EntityIndexConfiguration entityIndexConfiguration;

  /** Base mapping configuration loaded from external resource if specified. */
  private final Map<String, Object> mappingBaseConfiguration;

  /**
   * Constructs a new MultiEntityMappingsBuilder with the given entity index configuration.
   *
   * <p>This constructor initializes the builder and optionally loads base mapping configuration
   * from a resource file if specified in the configuration. The base configuration is merged with
   * generated mappings to provide system-level field definitions.
   *
   * @param entityIndexConfiguration the configuration containing v3 search settings and optional
   *     mapping configuration resource path
   * @throws IOException if there's an error loading the mapping configuration resource
   * @throws IllegalArgumentException if the configuration is null
   */
  public MultiEntityMappingsBuilder(@Nonnull EntityIndexConfiguration entityIndexConfiguration)
      throws IOException {

    this.entityIndexConfiguration = entityIndexConfiguration;
    String mappingConfig = entityIndexConfiguration.getV3().getMappingConfig();
    if (mappingConfig != null && !mappingConfig.trim().isEmpty()) {
      this.mappingBaseConfiguration =
          MultiEntityMappingsUtils.loadMappingConfigurationFromResource(mappingConfig);
    } else {
      this.mappingBaseConfiguration = null;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Generates index mappings for all search groups defined in the entity registry. Each search
   * group gets its own index with mappings that support all entities within that group.
   *
   * @param opContext the operation context containing entity registry and search configuration
   * @return collection of index mappings, one per search group, or empty if v3 is disabled
   */
  @Override
  public Collection<IndexMapping> getIndexMappings(@Nonnull OperationContext opContext) {
    return getIndexMappings(opContext, null);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Generates index mappings for all search groups with the specified structured properties.
   * This method creates mappings that include both entity fields and structured property fields for
   * comprehensive search capabilities.
   *
   * @param opContext the operation context containing entity registry and search configuration
   * @param structuredProperties collection of structured property definitions to include in
   *     mappings
   * @return collection of index mappings, one per search group, or empty if v3 is disabled
   */
  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    if (entityIndexConfiguration.getV3().isEnabled()) {
      // Generate Index Mapping per group
      return opContext.getEntityRegistry().getSearchGroups().stream()
          .map(
              searchGroup -> {
                Map<String, Object> mappings =
                    getMappingsForMultipleEntities(
                        opContext.getEntityRegistry(), searchGroup, structuredProperties);
                return IndexMapping.builder()
                    .indexName(
                        opContext
                            .getSearchContext()
                            .getIndexConvention()
                            .getEntityIndexNameV3(searchGroup))
                    .mappings(mappings)
                    .build();
              })
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Generates index mappings for a specific entity's search group when a new structured property
   * is added. This method is used for incremental updates when structured properties are created or
   * modified.
   *
   * @param opContext the operation context containing entity registry and search configuration
   * @param urn the URN of the entity for which to generate mappings
   * @param property the new structured property definition to include
   * @return collection containing a single index mapping for the entity's search group, or empty if
   *     v3 is disabled or entity has no search group
   */
  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {

    if (!entityIndexConfiguration.getV3().isEnabled()) {
      return Collections.emptyList();
    }

    List<IndexMapping> result = new ArrayList<>();

    // Get entity types from the property definition (e.g., urn:li:entityType:datahub.dataset)
    if (property.getEntityTypes() == null || property.getEntityTypes().isEmpty()) {
      log.warn("Property {} has no entity types defined", urn);
      return result;
    }

    // Group entity types by search group to build mappings per index
    Map<String, List<EntitySpec>> searchGroupToEntitySpecs = new HashMap<>();

    for (Urn entityTypeUrn : property.getEntityTypes()) {
      // Extract entity type name from URN, handling both formats:
      // - urn:li:entityType:dataset (legacy)
      // - urn:li:entityType:datahub.dataset (production)
      String entityTypeName = getEntityTypeId(entityTypeUrn);
      if (entityTypeName == null) {
        log.warn("Could not extract entity type from URN: {}", entityTypeUrn);
        continue;
      }

      EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(entityTypeName);

      if (entitySpec != null && entitySpec.getSearchGroup() != null) {
        String searchGroup = entitySpec.getSearchGroup();
        searchGroupToEntitySpecs
            .computeIfAbsent(searchGroup, k -> new ArrayList<>())
            .add(entitySpec);
      } else {
        log.warn("Missing entitySpec with searchGroup for entity type: {}", entityTypeName);
      }
    }

    // Build mappings for each search group
    for (Map.Entry<String, List<EntitySpec>> entry : searchGroupToEntitySpecs.entrySet()) {
      String searchGroup = entry.getKey();
      Map<String, Object> mappings =
          getMappingsForMultipleEntities(
              opContext.getEntityRegistry(), searchGroup, List.of(Pair.of(urn, property)));

      result.add(
          IndexMapping.builder()
              .indexName(
                  opContext
                      .getSearchContext()
                      .getIndexConvention()
                      .getEntityIndexNameV3(searchGroup))
              .mappings(mappings)
              .build());
    }

    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Generates Elasticsearch field mappings for structured properties based on their value types.
   * This method maps structured property value types to appropriate Elasticsearch field types for
   * indexing and searching.
   *
   * @param properties collection of structured property definitions with their associated URNs
   * @return map of field names to their Elasticsearch mapping configurations
   */
  @Override
  public Map<String, Object> getIndexMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    return properties.stream()
        .map(
            urnProperty -> {
              StructuredPropertyDefinition property = urnProperty.getSecond();
              Map<String, Object> mappingForField = new HashMap<>();
              LogicalValueType logicalType = getLogicalValueType(property.getValueType());

              switch (logicalType) {
                case STRING:
                case RICH_TEXT:
                  mappingForField = FieldTypeMapper.getMappingsForKeyword();
                  break;
                case DATE:
                  mappingForField.put(TYPE, ESUtils.DATE_FIELD_TYPE);
                  break;
                case URN:
                  mappingForField = FieldTypeMapper.getMappingsForUrn();
                  break;
                case NUMBER:
                  mappingForField.put(TYPE, ESUtils.DOUBLE_FIELD_TYPE);
                  break;
                default:
                  mappingForField = FieldTypeMapper.getMappingsForKeyword();
                  break;
              }

              return Map.entry(
                  toElasticsearchFieldName(urnProperty.getFirst(), property), mappingForField);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Builds mappings from multiple entity specs and a collection of structured properties. This
   * method aggregates mappings from all entities and merges them into a single mapping structure.
   * Fields are structured based on aspect names to provide better organization.
   *
   * <p>This method performs the following operations:
   *
   * <ul>
   *   <li>Extracts all entity specs for the given search group
   *   <li>Detects and resolves field name and type conflicts
   *   <li>Generates mappings for each entity spec
   *   <li>Merges all mappings into a unified structure
   *   <li>Creates root-level fields with copy_to for conflicted fields
   *   <li>Merges with base configuration if available
   *   <li>Builds the _search section for tier and label organization
   * </ul>
   *
   * @param entityRegistry entity registry containing all entity specifications
   * @param searchGroup the search group to get entities from
   * @param structuredProperties structured properties for all entities
   * @return combined mappings with aspect-based field structure for all entities
   * @throws IllegalArgumentException if the searchGroup does not exist in the registry
   */
  private Map<String, Object> getMappingsForMultipleEntities(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull String searchGroup,
      Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {

    // Extract entity specs from the registry based on searchGroup
    Collection<EntitySpec> entitySpecs =
        entityRegistry.getEntitySpecsBySearchGroup(searchGroup).values();

    if (entitySpecs.isEmpty()) {
      log.warn("No entities found for search group '{}'", searchGroup);
      return new HashMap<>();
    }

    // Detect conflicts using ConflictResolver
    ConflictResolver.ConflictResult conflictResult = ConflictResolver.detectConflicts(entitySpecs);
    Map<String, Set<String>> fieldNameConflicts = conflictResult.getFieldNameConflicts();
    Map<String, Set<String>> fieldNameAliasConflicts = conflictResult.getFieldNameAliasConflicts();

    if (conflictResult.hasConflicts()) {
      log.debug(
          "Detected conflicts - field name conflicts: {}, field name alias conflicts: {}",
          fieldNameConflicts.size(),
          fieldNameAliasConflicts.size());
    }

    // Resolve field type conflicts by choosing the most appropriate type
    if (conflictResult.hasTypeConflicts()) {
      log.debug(
          "Resolving field type conflicts. Field name conflicts: {}, Alias conflicts: {}",
          conflictResult.getFieldNameTypeConflicts(),
          conflictResult.getFieldNameAliasTypeConflicts());

      // Try to resolve conflicts, but throw exception for non-resolvable conflicts
      try {
        // Log the resolution for each conflict
        for (Map.Entry<String, Set<String>> entry :
            conflictResult.getFieldNameTypeConflicts().entrySet()) {
          String fieldName = entry.getKey();
          Set<String> conflictingTypes = entry.getValue();
          String resolvedType = ConflictResolver.resolveTypeConflict(conflictingTypes);
          log.debug(
              "Resolved field '{}' type conflict {} -> {}",
              fieldName,
              conflictingTypes,
              resolvedType);
        }

        for (Map.Entry<String, Set<String>> entry :
            conflictResult.getFieldNameAliasTypeConflicts().entrySet()) {
          String alias = entry.getKey();
          Set<String> conflictingTypes = entry.getValue();
          String resolvedType = ConflictResolver.resolveTypeConflict(conflictingTypes);
          log.info(
              "Resolved alias '{}' type conflict {} -> {}", alias, conflictingTypes, resolvedType);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Non-resolvable field type conflicts detected in search group '%s'. %s",
                searchGroup, e.getMessage()),
            e);
      }
    }

    Map<String, Object> combinedMappings = new HashMap<>();

    // Process each entity spec and combine their mappings
    for (EntitySpec entitySpec : entitySpecs) {
      Map<String, Object> entityMappings =
          getIndexMappings(
              entityRegistry,
              entitySpec,
              structuredProperties,
              fieldNameConflicts,
              fieldNameAliasConflicts);
      combinedMappings = MultiEntityMappingsUtils.mergeMappings(combinedMappings, entityMappings);
    }

    // Create root-level fields with copy_to from aspect fields
    Map<String, Object> rootFieldsWithCopyTo =
        createRootFieldsWithCopyTo(entitySpecs, fieldNameConflicts, fieldNameAliasConflicts);
    if (!rootFieldsWithCopyTo.isEmpty()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) combinedMappings.get("properties");
      if (properties != null) {
        properties.putAll(rootFieldsWithCopyTo);
      }
    }

    // Merge with base configuration if available
    if (mappingBaseConfiguration != null) {
      combinedMappings =
          MultiEntityMappingsUtils.mergeMappings(combinedMappings, mappingBaseConfiguration);
    }

    // Build _search section with all copy_to destination fields
    Map<String, Object> searchSection =
        MultiEntityMappingsUtils.buildSearchSection(entitySpecs, combinedMappings);
    if (!searchSection.isEmpty()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) combinedMappings.get("properties");
      if (properties != null) {
        properties.put("_search", searchSection);
      }
    }

    return combinedMappings;
  }

  /**
   * Builds mappings from entity spec with aspect-based field structure. This is the main method
   * that handles all mapping generation scenarios for a single entity.
   *
   * <p>This method creates mappings with the following structure:
   *
   * <ul>
   *   <li>Aspect mappings under {@code _aspects} object using AspectMappingBuilder
   *   <li>Searchable reference field mappings for related entities
   *   <li>Root-level aliases for single-aspect fields
   *   <li>Structured property mappings under {@code structuredProperties} field
   *   <li>System fields from base configuration (merged separately)
   * </ul>
   *
   * @param entityRegistry entity registry containing entity specifications
   * @param entitySpec entity's specification containing aspect and field definitions
   * @param structuredProperties structured properties for the entity (optional)
   * @param fieldNameConflicts map of field names that have conflicts (optional)
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts (optional)
   * @return mappings with aspect-based field structure
   */
  private Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final EntitySpec entitySpec,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties,
      @Nullable Map<String, Set<String>> fieldNameConflicts,
      @Nullable Map<String, Set<String>> fieldNameAliasConflicts) {

    // Use empty collections as defaults for null parameters
    if (structuredProperties == null) {
      structuredProperties = Collections.emptyList();
    }
    final Map<String, Set<String>> finalFieldNameConflicts =
        fieldNameConflicts != null ? fieldNameConflicts : Collections.emptyMap();
    if (fieldNameAliasConflicts == null) {
      fieldNameAliasConflicts = Collections.emptyMap();
    }
    Map<String, Object> mappings = new HashMap<>();

    // Create aspect mappings using AspectMappingBuilder
    Map<String, Object> aspectMappings =
        AspectMappingBuilder.createAspectMappings(
            entitySpec, finalFieldNameConflicts, fieldNameAliasConflicts);

    // Log aspect mappings
    for (Map.Entry<String, Object> entry : aspectMappings.entrySet()) {
      String aspectName = entry.getKey();
      @SuppressWarnings("unchecked")
      Map<String, Object> aspectFields =
          (Map<String, Object>) ((Map<String, Object>) entry.getValue()).get(PROPERTIES);
      log.debug(
          "Added aspect '{}' with {} fields to _aspects: {}",
          aspectName,
          aspectFields.size(),
          aspectFields.keySet());
    }

    final Map<String, Object> finalAspectsMappings = aspectMappings;

    // Process searchable ref fields - they will be grouped under _aspects
    entitySpec
        .getSearchableRefFieldSpecs()
        .forEach(
            searchableRefFieldSpec -> {
              finalAspectsMappings.putAll(
                  getMappingForSearchableRefField(
                      entityRegistry,
                      searchableRefFieldSpec,
                      searchableRefFieldSpec.getSearchableRefAnnotation().getDepth()));
            });

    // Add _aspects object to root mappings
    if (!finalAspectsMappings.isEmpty()) {
      mappings.put(
          MappingConstants.ASPECTS_FIELD_NAME, ImmutableMap.of(PROPERTIES, finalAspectsMappings));
    }

    // Create root-level aliases for all searchable fields using AspectMappingBuilder
    Map<String, Object> rootAliases =
        AspectMappingBuilder.createRootLevelAliases(entitySpec, fieldNameAliasConflicts);
    mappings.putAll(rootAliases);

    // Process structured properties using StructuredPropertyMappingBuilder
    Map<String, Object> structuredPropertyMappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            entitySpec, structuredProperties);

    // Add structured properties from parameters under a structuredProperties container
    // Always create the structuredProperties field as dynamic to handle future structured
    // properties
    mappings.put(
        STRUCTURED_PROPERTY_MAPPING_FIELD,
        ImmutableMap.of(
            TYPE,
            ESUtils.OBJECT_FIELD_TYPE,
            "dynamic",
            true,
            PROPERTIES,
            structuredPropertyMappings.isEmpty() ? new HashMap<>() : structuredPropertyMappings));

    // Note: Root level system fields (urn, runId, systemCreated, systemIndexModified, _entityType,
    // _search)
    // are now defined in the base configuration YAML file and will be merged automatically

    return ImmutableMap.of(PROPERTIES, mappings);
  }

  /**
   * Gets mappings for a single entity spec without structured properties or conflicts. This method
   * is used by ESUtils.buildSearchableFieldTypes to extract field types from mappings for backward
   * compatibility and utility purposes.
   *
   * @param entityRegistry entity registry containing entity specifications
   * @param entitySpec entity specification to get mappings for
   * @return mappings for the entity spec with aspect-based field structure
   */
  public Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    return getIndexMappings(entityRegistry, entitySpec, null, null, null);
  }

  /**
   * Convenience method for building mappings with structured properties but no conflicts. This
   * method provides a simplified interface for cases where conflict resolution is not needed.
   *
   * @param entityRegistry entity registry containing entity specifications
   * @param entitySpec entity specification containing aspect and field definitions
   * @param structuredProperties structured properties for the entity
   * @return mappings with aspect-based field structure
   */
  private Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final EntitySpec entitySpec,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    return getIndexMappings(entityRegistry, entitySpec, structuredProperties, null, null);
  }

  /**
   * Collects all field names and their paths from an entity spec. This method extracts field paths
   * for non-structuredProperties aspects, excluding MAP_ARRAY object fields that are handled
   * separately.
   *
   * @param entitySpec the entity specification to collect field paths from
   * @return map of field names to their aspect-based paths (e.g., "_aspects.ownership.owners")
   */
  private static Map<String, String> collectFieldPaths(@Nonnull final EntitySpec entitySpec) {
    Map<String, String> fieldPaths = new HashMap<>();

    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      if (!"structuredProperties".equals(aspectSpec.getName())) {
        collectFieldPathsFromAspect(aspectSpec, fieldPaths);
      }
    }

    return fieldPaths;
  }

  /**
   * Collects field paths from a single aspect specification. This helper method extracts field
   * names and their aspect-based paths, excluding object field types that are handled as root-level
   * aliases.
   *
   * <p>Note: If the same field name appears in multiple aspects within the same entity, only the
   * last occurrence is kept (overwrites previous). This is intentional behavior for conflict
   * detection - we only need one representative path per field name per entity. The actual conflict
   * detection happens at the multi-entity level in collectAllFieldPaths.
   *
   * @param aspectSpec the aspect specification to collect field paths from
   * @param fieldPaths map to populate with field names and their paths
   */
  private static void collectFieldPathsFromAspect(
      @Nonnull AspectSpec aspectSpec, @Nonnull Map<String, String> fieldPaths) {
    String aspectName = aspectSpec.getName();

    for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();

      // Skip object field types (root-level aliases)
      if (OBJECT_FIELD_TYPES.contains(fieldType)) {
        continue;
      }

      String fieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
      String aspectFieldPath = "_aspects." + aspectName + "." + fieldName;

      // Note: If the same field name appears in multiple aspects within the same entity,
      // this will overwrite the previous path. This is intentional behavior for conflict
      // detection - we only need one representative path per field name per entity.
      // The actual conflict detection happens at the multi-entity level in collectAllFieldPaths.
      fieldPaths.put(fieldName, aspectFieldPath);
    }
  }

  /**
   * Creates root-level fields based on field name conflicts. For fields with conflicts, creates a
   * root field that aspect fields copy_to. For fields without conflicts, creates a root alias
   * pointing to the single aspect field.
   *
   * <p>This method handles both regular field names and field name aliases, creating appropriate
   * root-level mappings that allow unified search across all aspects while maintaining aspect-based
   * organization.
   *
   * @param entitySpecs all entity specifications in the search group
   * @param fieldNameConflicts map of field names that have conflicts across aspects
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts across aspects
   * @return map of root fields with appropriate configuration (alias or field definition)
   */
  private static Map<String, Object> createRootFieldsWithCopyTo(
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Map<String, Set<String>> fieldNameConflicts,
      @Nonnull Map<String, Set<String>> fieldNameAliasConflicts) {

    Map<String, Object> rootFields = new HashMap<>();

    // Collect all field paths from all entities
    Map<String, Set<String>> fieldNameToAllPaths = collectAllFieldPaths(entitySpecs);
    Map<String, Set<String>> fieldNameAliasToAllPaths = collectAllFieldNameAliasPaths(entitySpecs);

    // Create root fields for regular field names
    createRootFieldsForFieldNames(rootFields, fieldNameToAllPaths, fieldNameConflicts, entitySpecs);

    // Create root fields for field name aliases
    createRootFieldsForFieldNameAliases(
        rootFields,
        fieldNameAliasToAllPaths,
        fieldNameAliasConflicts,
        fieldNameConflicts,
        entitySpecs);

    return rootFields;
  }

  /**
   * Collects all field paths from all entity specifications in a search group. This method
   * aggregates field paths across all entities to identify which fields appear in multiple aspects
   * and require conflict resolution.
   *
   * @param entitySpecs collection of entity specifications in the search group
   * @return map of field names to sets of all paths where they appear
   */
  private static Map<String, Set<String>> collectAllFieldPaths(
      @Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, Set<String>> fieldNameToAllPaths = new HashMap<>();

    for (EntitySpec entitySpec : entitySpecs) {
      Map<String, String> entityFieldPaths = collectFieldPaths(entitySpec);
      for (Map.Entry<String, String> entry : entityFieldPaths.entrySet()) {
        String fieldName = entry.getKey();
        String fieldPath = entry.getValue();
        fieldNameToAllPaths.computeIfAbsent(fieldName, k -> new HashSet<>()).add(fieldPath);
      }
    }

    return fieldNameToAllPaths;
  }

  /**
   * Collects all field name alias paths from all entity specifications in a search group. This
   * method aggregates field name alias paths across all entities to identify which aliases appear
   * in multiple aspects and require conflict resolution.
   *
   * @param entitySpecs collection of entity specifications in the search group
   * @return map of field name aliases to sets of all paths where they appear
   */
  private static Map<String, Set<String>> collectAllFieldNameAliasPaths(
      @Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, Set<String>> fieldNameAliasToAllPaths = new HashMap<>();

    for (EntitySpec entitySpec : entitySpecs) {
      Map<String, Set<String>> entityFieldNameAliasPaths =
          ConflictResolver.collectFieldNameAliasPaths(entitySpec);
      for (Map.Entry<String, Set<String>> entry : entityFieldNameAliasPaths.entrySet()) {
        String alias = entry.getKey();
        Set<String> fieldPaths = entry.getValue();
        fieldNameAliasToAllPaths.computeIfAbsent(alias, k -> new HashSet<>()).addAll(fieldPaths);
      }
    }

    return fieldNameAliasToAllPaths;
  }

  /**
   * Creates root fields for regular field names based on conflict analysis. For conflicted fields,
   * creates root fields with resolved types. For non-conflicted fields, creates aliases pointing to
   * their single aspect location.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldNameToAllPaths map of field names to all their paths
   * @param fieldNameConflicts map of field names that have conflicts
   * @param entitySpecs all entity specifications for type resolution
   */
  private static void createRootFieldsForFieldNames(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull Map<String, Set<String>> fieldNameToAllPaths,
      @Nonnull Map<String, Set<String>> fieldNameConflicts,
      @Nonnull Collection<EntitySpec> entitySpecs) {

    for (Map.Entry<String, Set<String>> entry : fieldNameToAllPaths.entrySet()) {
      String fieldName = entry.getKey();
      Set<String> allPaths = entry.getValue();

      if (fieldNameConflicts.containsKey(fieldName)) {
        // Field appears in multiple aspects - create non-alias field with copy_to from all paths
        createConflictedFieldMapping(rootFields, fieldName, allPaths, entitySpecs);
      } else {
        // Field appears in only one aspect - create alias pointing to that single path
        createSingleFieldAlias(rootFields, fieldName, allPaths);
      }
    }
  }

  /**
   * Creates root fields for field name aliases based on conflict analysis. Handles both conflicted
   * and non-conflicted aliases, with special handling for aliases that conflict with regular field
   * names.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldNameAliasToAllPaths map of field name aliases to all their paths
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts
   * @param fieldNameConflicts map of field names that have conflicts (for overlap detection)
   * @param entitySpecs all entity specifications for type resolution
   */
  private static void createRootFieldsForFieldNameAliases(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull Map<String, Set<String>> fieldNameAliasToAllPaths,
      @Nonnull Map<String, Set<String>> fieldNameAliasConflicts,
      @Nonnull Map<String, Set<String>> fieldNameConflicts,
      @Nonnull Collection<EntitySpec> entitySpecs) {

    for (Map.Entry<String, Set<String>> entry : fieldNameAliasToAllPaths.entrySet()) {
      String alias = entry.getKey();
      Set<String> allPaths = entry.getValue();

      if (fieldNameAliasConflicts.containsKey(alias)) {
        createConflictedFieldNameAliasMapping(
            rootFields, alias, allPaths, fieldNameConflicts, entitySpecs);
      } else {
        createSingleFieldNameAlias(rootFields, alias, allPaths);
      }
    }
  }

  /**
   * Creates a mapping for a field that has conflicts (appears in multiple aspects). Resolves type
   * conflicts and creates a root field that aspect fields will copy_to. Special handling for
   * _entityName field which creates an alias to _search.entityName.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldName the conflicted field name
   * @param allPaths all paths where this field appears
   * @param entitySpecs all entity specifications for type resolution
   */
  private static void createConflictedFieldMapping(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull String fieldName,
      @Nonnull Set<String> allPaths,
      @Nonnull Collection<EntitySpec> entitySpecs) {

    // Find all field types for this field name across all paths
    Set<FieldType> fieldTypes = new HashSet<>();
    Set<String> elasticsearchTypes = new HashSet<>();
    for (String path : allPaths) {
      String aspectName = path.split("\\.")[1]; // Extract aspect name from path
      FieldType fieldType =
          MultiEntityMappingsUtils.findFieldTypeForFieldName(entitySpecs, fieldName, aspectName);
      fieldTypes.add(fieldType);
      elasticsearchTypes.add(FieldTypeMapper.getElasticsearchTypeForFieldType(fieldType));
    }

    // Resolve type conflicts using the conflict resolver
    String resolvedElasticsearchType;
    if (elasticsearchTypes.size() > 1) {
      try {
        resolvedElasticsearchType = ConflictResolver.resolveTypeConflict(elasticsearchTypes);
        log.debug(
            "Resolved field '{}' type conflict {} -> {}",
            fieldName,
            elasticsearchTypes,
            resolvedElasticsearchType);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Non-resolvable field type conflict for field '%s' with types %s. %s",
                fieldName, elasticsearchTypes, e.getMessage()),
            e);
      }
    } else {
      resolvedElasticsearchType = elasticsearchTypes.iterator().next();
    }

    // Special handling for _entityName field - create alias to _search.entityName
    if (MultiEntityMappingsUtils.isEntityNameField(fieldName)) {
      rootFields.put(fieldName, MultiEntityMappingsUtils.createEntityNameAliasMapping());
      log.debug("Creating root alias for conflicted _entityName -> _search.entityName");
    } else {
      // Create root field mapping - this is the target field that aspect fields will copy_to
      Map<String, Object> rootFieldMapping = new HashMap<>();
      rootFieldMapping.put(TYPE, resolvedElasticsearchType);

      // Check if any of the conflicting fields have eagerGlobalOrdinals set to true
      boolean hasEagerGlobalOrdinals =
          MultiEntityMappingsUtils.hasEagerGlobalOrdinals(entitySpecs, fieldName, allPaths);
      if (hasEagerGlobalOrdinals) {
        rootFieldMapping.put("eager_global_ordinals", true);
        log.debug("Setting eager_global_ordinals=true for conflicted root field '{}'", fieldName);
      }

      // Root field should NOT have copy_to - it's the target for aspect fields to copy to
      // Aspect fields will have copy_to pointing to this root field

      rootFields.put(fieldName, rootFieldMapping);
    }

    log.debug(
        "Creating root field for conflicted field: '{}' (target for aspect fields to copy_to)",
        fieldName);
  }

  /**
   * Creates an alias for a field that has no conflicts (appears in only one aspect). The alias
   * points directly to the single aspect field location.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldName the non-conflicted field name
   * @param allPaths set containing the single path where this field appears
   */
  private static void createSingleFieldAlias(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull String fieldName,
      @Nonnull Set<String> allPaths) {

    String singlePath = allPaths.iterator().next();

    Map<String, Object> aliasMapping = new HashMap<>();
    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
    aliasMapping.put(PATH, singlePath);

    rootFields.put(fieldName, aliasMapping);

    log.debug("Creating root alias for single field: '{}' -> '{}'", fieldName, singlePath);
  }

  /**
   * Creates a mapping for a field name alias that has conflicts. Handles special cases where the
   * alias conflicts with a regular field name, and creates appropriate root field mappings with
   * type resolution.
   *
   * @param rootFields map to populate with root field configurations
   * @param alias the conflicted field name alias
   * @param allPaths all paths where this alias appears
   * @param fieldNameConflicts map of field names that have conflicts (for overlap detection)
   * @param entitySpecs all entity specifications for type resolution
   */
  private static void createConflictedFieldNameAliasMapping(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull String alias,
      @Nonnull Set<String> allPaths,
      @Nonnull Map<String, Set<String>> fieldNameConflicts,
      @Nonnull Collection<EntitySpec> entitySpecs) {

    // Check if this alias conflicts with a field name
    if (fieldNameConflicts.containsKey(alias)) {
      // This alias conflicts with a field name, so the field name processing already created
      // the root field with copy_to from all conflicting sources. We skip here to avoid
      // duplicate field creation.
      log.debug(
          "Skipping conflicted field name alias '{}' as it conflicts with field name (already handled)",
          alias);
      return;
    }

    String firstPath = allPaths.iterator().next();
    String aspectName = firstPath.split("\\.")[1]; // Extract aspect name from path

    // Find the field type from the entity specs using the actual field name from the path
    String actualFieldName = firstPath.split("\\.")[2]; // Extract field name from path
    FieldType fieldType =
        MultiEntityMappingsUtils.findFieldTypeForFieldName(
            entitySpecs, actualFieldName, aspectName);

    // Special handling for _entityName alias - create alias to _search.entityName
    if (MultiEntityMappingsUtils.isEntityNameField(alias)) {
      rootFields.put(alias, MultiEntityMappingsUtils.createEntityNameAliasMapping());
      log.debug("Creating root alias for _entityName -> _search.entityName");
    } else {
      // Create root field mapping - this is the target field that aspect fields will copy_to
      Map<String, Object> rootFieldMapping = new HashMap<>();
      rootFieldMapping.put(TYPE, FieldTypeMapper.getElasticsearchTypeForFieldType(fieldType));

      // Check if any of the conflicting fields have eagerGlobalOrdinals set to true
      boolean hasEagerGlobalOrdinals =
          MultiEntityMappingsUtils.hasEagerGlobalOrdinals(entitySpecs, actualFieldName, allPaths);
      if (hasEagerGlobalOrdinals) {
        rootFieldMapping.put("eager_global_ordinals", true);
        log.debug("Setting eager_global_ordinals=true for conflicted field name alias '{}'", alias);
      }

      // Root field should NOT have copy_to - it's the target for aspect fields to copy to
      // Aspect fields will have copy_to pointing to this root field

      rootFields.put(alias, rootFieldMapping);
    }

    log.debug(
        "Creating root field for conflicted field name alias: '{}' (target for aspect fields to copy_to)",
        alias);
  }

  /**
   * Creates an alias for a field name alias that has no conflicts. The alias points directly to the
   * single aspect field location.
   *
   * @param rootFields map to populate with root field configurations
   * @param alias the non-conflicted field name alias
   * @param allPaths set containing the single path where this alias appears
   */
  private static void createSingleFieldNameAlias(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull String alias,
      @Nonnull Set<String> allPaths) {

    String singlePath = allPaths.iterator().next();

    Map<String, Object> aliasMapping = new HashMap<>();
    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
    aliasMapping.put(PATH, singlePath);

    rootFields.put(alias, aliasMapping);

    log.debug("Creating root alias for single field name alias: '{}' -> '{}'", alias, singlePath);
  }

  /**
   * Gets Elasticsearch mappings for a single searchable field specification. This method creates
   * field mappings with appropriate type, indexing, and copy_to configurations based on the field's
   * annotations and conflict status.
   *
   * <p>This is a convenience method that calls the full version with empty conflict maps.
   *
   * @param searchableFieldSpec the field specification to create mappings for
   * @param aspectName the aspect name where this field is located
   * @return map containing the field mapping configuration
   */
  public static Map<String, Object> getMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec, @Nonnull final String aspectName) {
    return getMappingsForField(searchableFieldSpec, aspectName, Collections.emptyMap(), null);
  }

  /**
   * Gets Elasticsearch mappings for a single searchable field specification with conflict handling.
   * This method creates comprehensive field mappings including:
   *
   * <ul>
   *   <li>Field type mapping based on annotations
   *   <li>Eager global ordinals configuration
   *   <li>Search tier and label copy_to fields
   *   <li>Entity field name copy_to fields
   *   <li>Conflict resolution copy_to fields
   *   <li>HasValues and numValues field creation
   *   <li>SystemModifiedAt field creation
   * </ul>
   *
   * @param searchableFieldSpec the field specification to create mappings for
   * @param aspectName the aspect name where this field is located
   * @param fieldNameConflicts map of field names that have conflicts (optional)
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts (optional)
   * @return map containing the field mapping configuration
   */
  public static Map<String, Object> getMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec,
      @Nonnull final String aspectName,
      @Nullable final Map<String, Set<String>> fieldNameConflicts,
      @Nullable final Map<String, Set<String>> fieldNameAliasConflicts) {
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();
    String baseFieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();

    String actualFieldName = baseFieldName;

    // Handle null parameters
    final Map<String, Set<String>> finalFieldNameConflicts =
        fieldNameConflicts != null ? fieldNameConflicts : Collections.emptyMap();
    final Map<String, Set<String>> finalFieldNameAliasConflicts =
        fieldNameAliasConflicts != null ? fieldNameAliasConflicts : Collections.emptyMap();

    log.debug(
        "Processing field '{}' of type '{}' for aspect '{}'", baseFieldName, fieldType, aspectName);

    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();

    // Use FieldTypeMapper to get the appropriate mapping for the field type
    // Use the enhanced mapping that considers the underlying PDL field type for more precise
    // numeric types
    mappingForField.putAll(FieldTypeMapper.getMappingsForFieldType(fieldType, searchableFieldSpec));

    // Handle eagerGlobalOrdinals - set eager_global_ordinals to true if specified and field type is
    // appropriate
    searchableFieldSpec
        .getSearchableAnnotation()
        .getEagerGlobalOrdinals()
        .ifPresent(
            eagerGlobalOrdinals -> {
              if (eagerGlobalOrdinals) {
                // Only apply eager_global_ordinals to appropriate field types
                if (fieldType == FieldType.KEYWORD
                    || fieldType == FieldType.URN
                    || fieldType == FieldType.URN_PARTIAL) {
                  mappingForField.put("eager_global_ordinals", true);
                  log.debug("Setting eager_global_ordinals=true for field '{}'", baseFieldName);
                } else {
                  log.debug(
                      "Skipping eager_global_ordinals for field '{}' with field type '{}' (not supported)",
                      baseFieldName,
                      fieldType);
                }
              }
            });

    // Handle tier annotations - add copy_to field if searchTier is specified
    searchableFieldSpec
        .getSearchableAnnotation()
        .getSearchTier()
        .ifPresent(
            tier -> {
              if (tier >= 1) {
                List<String> copyTo = new ArrayList<>();
                copyTo.add("_search.tier_" + tier);
                mappingForField.put(COPY_TO, copyTo);

                // Respect searchIndexed annotation if specified
                searchableFieldSpec
                    .getSearchableAnnotation()
                    .getSearchIndexed()
                    .ifPresent(
                        searchIndexed -> {
                          if (searchIndexed) {
                            // If searchIndexed is true, ensure the field is indexed as KEYWORD
                            mappingForField.put(TYPE, ESUtils.KEYWORD_FIELD_TYPE);
                            mappingForField.put(INDEX, true);
                          } else {
                            // If searchIndexed is false, set index to false
                            mappingForField.put(INDEX, false);
                          }
                        });
              }
            });

    // Handle searchLabel annotations - add copy_to field if searchLabel is specified
    searchableFieldSpec
        .getSearchableAnnotation()
        .getSearchLabel()
        .ifPresent(
            searchLabel -> {
              if (searchLabel != null && !searchLabel.isEmpty()) {
                List<String> copyTo =
                    (List<String>) mappingForField.getOrDefault(COPY_TO, new ArrayList<>());
                copyTo.add("_search." + searchLabel);
                mappingForField.put(COPY_TO, copyTo);
              }
            });

    // Handle entityFieldName annotations - add copy_to field if entityFieldName is specified
    searchableFieldSpec
        .getSearchableAnnotation()
        .getEntityFieldName()
        .ifPresent(
            entityFieldName -> {
              if (entityFieldName != null && !entityFieldName.isEmpty()) {
                List<String> copyTo =
                    (List<String>) mappingForField.getOrDefault(COPY_TO, new ArrayList<>());
                copyTo.add("_search." + entityFieldName);
                mappingForField.put(COPY_TO, copyTo);
              }
            });

    // Create field directly under aspect name (not prefixed)
    // For MAP_ARRAY fields with "/$key" field name, use the schema field name instead
    String fieldName = actualFieldName;
    if ("/$key".equals(actualFieldName)
        && searchableFieldSpec.getSearchableAnnotation().getFieldType() == FieldType.MAP_ARRAY) {
      fieldName = FieldSpecUtils.getSchemaFieldName(searchableFieldSpec.getPath());
    }
    mappings.put(fieldName, mappingForField);

    // Add hasValues and numValues fields if specified
    searchableFieldSpec
        .getSearchableAnnotation()
        .getHasValuesFieldName()
        .ifPresent(
            hasValuesFieldName -> {
              mappings.put(hasValuesFieldName, ImmutableMap.of(TYPE, ESUtils.BOOLEAN_FIELD_TYPE));
            });
    searchableFieldSpec
        .getSearchableAnnotation()
        .getNumValuesFieldName()
        .ifPresent(
            numValuesFieldName -> {
              mappings.put(numValuesFieldName, ImmutableMap.of(TYPE, ESUtils.LONG_FIELD_TYPE));
            });

    // Add systemModifiedAt field if specified
    if (ESUtils.getSystemModifiedAtFieldName(searchableFieldSpec).isPresent()) {
      String modifiedAtFieldName = ESUtils.getSystemModifiedAtFieldName(searchableFieldSpec).get();
      mappings.put(modifiedAtFieldName, ImmutableMap.of(TYPE, ESUtils.DATE_FIELD_TYPE));
    }

    // Note: Field name aliases are now handled at the root level in createRootLevelAliases
    // to avoid creating aliases inside the _aspects structure

    // Add copy_to to root field if this field has conflicts (but not for structuredProperties)
    if (finalFieldNameConflicts.containsKey(baseFieldName)
        && !"structuredProperties".equals(aspectName)) {
      List<String> rootCopyTo =
          (List<String>) mappingForField.getOrDefault(COPY_TO, new ArrayList<>());
      rootCopyTo.add(baseFieldName); // Copy to the root field with the same name
      mappingForField.put(COPY_TO, rootCopyTo);

      log.debug(
          "Adding copy_to to root field '{}' for conflicted field in aspect '{}'",
          baseFieldName,
          aspectName);
    }

    // Add copy_to to root field if this field's aliases have conflicts (but not for
    // structuredProperties)
    if (!finalFieldNameAliasConflicts.isEmpty() && !"structuredProperties".equals(aspectName)) {
      List<String> fieldNameAliases =
          searchableFieldSpec.getSearchableAnnotation().getFieldNameAliases();
      for (String alias : fieldNameAliases) {
        if (finalFieldNameAliasConflicts.containsKey(alias)) {
          List<String> aliasCopyTo =
              (List<String>) mappingForField.getOrDefault(COPY_TO, new ArrayList<>());
          aliasCopyTo.add(alias); // Copy to the root field with the alias name
          mappingForField.put(COPY_TO, aliasCopyTo);

          log.debug(
              "Adding copy_to to root field '{}' for conflicted field alias in aspect '{}'",
              alias,
              aspectName);
        }
      }
    }

    // Filter out alias fields from copy_to arrays since Elasticsearch doesn't support copy_to
    // aliases
    // This must be done at the very end after all copy_to additions
    List<String> finalCopyTo =
        (List<String>) mappingForField.getOrDefault(COPY_TO, new ArrayList<>());
    if (!finalCopyTo.isEmpty()) {
      List<String> filteredCopyTo =
          finalCopyTo.stream()
              .filter(destination -> !MultiEntityMappingsUtils.isEntityNameField(destination))
              .collect(Collectors.toList());

      if (filteredCopyTo.size() != finalCopyTo.size()) {
        log.debug(
            "Filtered out alias fields from copy_to for field '{}': {} -> {}",
            baseFieldName,
            finalCopyTo,
            filteredCopyTo);
        if (filteredCopyTo.isEmpty()) {
          mappingForField.remove(COPY_TO);
        } else {
          mappingForField.put(COPY_TO, filteredCopyTo);
        }
      }
    }

    return mappings;
  }

  /**
   * Gets mappings for a searchable reference field that references other entities. This method
   * creates nested mappings for referenced entity fields up to the specified depth. For depth 0,
   * creates a simple URN field. For depth > 0, creates nested object mappings containing the
   * referenced entity's searchable fields.
   *
   * @param entityRegistry entity registry for resolving referenced entity specifications
   * @param searchableRefFieldSpec the reference field specification
   * @param depth the maximum depth of nested references to include
   * @return map containing the reference field mapping configuration
   */
  private static Map<String, Object> getMappingForSearchableRefField(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final SearchableRefFieldSpec searchableRefFieldSpec,
      @Nonnull final int depth) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    Map<String, Object> mappingForProperty = new HashMap<>();

    String baseFieldName = searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName();

    if (depth == 0) {
      mappings.put(baseFieldName, FieldTypeMapper.getMappingsForUrn());
      return mappings;
    }

    String entityType = searchableRefFieldSpec.getSearchableRefAnnotation().getRefType();
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);

    entitySpec
        .getSearchableFieldSpecs()
        .forEach(
            searchableFieldSpec ->
                mappingForField.putAll(
                    getMappingsForField(searchableFieldSpec, "ref_" + entityType)));
    // Process searchable reference fields recursively
    for (SearchableRefFieldSpec refFieldSpec : entitySpec.getSearchableRefFieldSpecs()) {
      int configuredDepth = refFieldSpec.getSearchableRefAnnotation().getDepth();
      int remainingDepth = Math.min(depth - 1, configuredDepth);

      Map<String, Object> refFieldMappings =
          getMappingForSearchableRefField(entityRegistry, refFieldSpec, remainingDepth);

      mappingForField.putAll(refFieldMappings);
    }

    mappingForField.put("urn", FieldTypeMapper.getMappingsForUrn());
    mappingForProperty.put("properties", mappingForField);

    mappings.put(baseFieldName, mappingForProperty);
    return mappings;
  }
}
