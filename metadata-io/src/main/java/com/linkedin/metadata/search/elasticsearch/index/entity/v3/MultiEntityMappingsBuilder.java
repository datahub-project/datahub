package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
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

@Slf4j
public class MultiEntityMappingsBuilder implements MappingsBuilder {

  private final EntityIndexConfiguration entityIndexConfiguration;
  private final Map<String, Object> mappingBaseConfiguration;

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

  @Override
  public Collection<IndexMapping> getMappings(@Nonnull OperationContext opContext) {
    return getIndexMappings(opContext, null);
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
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

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    List<IndexMapping> result = new ArrayList<>(1);

    if (entityIndexConfiguration.getV3().isEnabled()) {
      EntitySpec entitySpec = opContext.getEntityRegistry().getEntitySpec(urn.getEntityType());

      if (entitySpec != null && entitySpec.getSearchGroup() != null) {
        String searchGroup = entitySpec.getSearchGroup();
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
      } else {
        log.warn("Missing entitySpec with searchGroup for {}", urn.getEntityType());
      }
    }

    return result;
  }

  @Override
  public Map<String, Object> getMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    return properties.stream()
        .map(
            urnProperty -> {
              StructuredPropertyDefinition property = urnProperty.getSecond();
              Map<String, Object> mappingForField = new HashMap<>();
              String valueType = property.getValueType().getId();

              // Map structured property value types to field types
              if (valueType.equalsIgnoreCase(LogicalValueType.STRING.name())
                  || valueType.equalsIgnoreCase(LogicalValueType.RICH_TEXT.name())) {
                mappingForField = FieldTypeMapper.getMappingsForKeyword();
              } else if (valueType.equalsIgnoreCase(LogicalValueType.DATE.name())) {
                mappingForField.put(TYPE, ESUtils.DATE_FIELD_TYPE);
              } else if (valueType.equalsIgnoreCase(LogicalValueType.URN.name())) {
                mappingForField = FieldTypeMapper.getMappingsForUrn();
              } else if (valueType.equalsIgnoreCase(LogicalValueType.NUMBER.name())) {
                mappingForField.put(TYPE, ESUtils.DOUBLE_FIELD_TYPE);
              } else {
                // Default to keyword for unknown types
                mappingForField = FieldTypeMapper.getMappingsForKeyword();
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
   * @param entityRegistry entity registry
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
        // Re-throw the exception for non-resolvable conflicts
        throw e;
      }
    }

    Map<String, Object> combinedMappings = new HashMap<>();

    // Process each entity spec and combine their mappings
    for (EntitySpec entitySpec : entitySpecs) {
      Map<String, Object> entityMappings =
          getMappings(
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
   * that handles all mapping generation scenarios.
   *
   * @param entityRegistry entity registry
   * @param entitySpec entity's spec
   * @param structuredProperties structured properties for the entity (optional)
   * @param fieldNameConflicts map of field names that have conflicts (optional)
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts (optional)
   * @return mappings with aspect-based field structure
   */
  private Map<String, Object> getMappings(
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
   * Method to get mappings for a single entity spec. This method is used by
   * ESUtils.buildSearchableFieldTypes to extract field types from mappings.
   *
   * @param entityRegistry entity registry
   * @param entitySpec entity spec to get mappings for
   * @return mappings for the entity spec
   */
  public Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    return getMappings(entityRegistry, entitySpec, null, null, null);
  }

  /**
   * Convenience method for building mappings with structured properties but no conflicts.
   *
   * @param entityRegistry entity registry
   * @param entitySpec entity's spec
   * @param structuredProperties structured properties for the entity
   * @return mappings with aspect-based field structure
   */
  private Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final EntitySpec entitySpec,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    return getMappings(entityRegistry, entitySpec, structuredProperties, null, null);
  }

  /**
   * Collects all field names and their paths from an entity spec.
   *
   * @param entitySpec the entity spec to collect field paths from
   * @return map of field names to their paths
   */
  private static Map<String, String> collectFieldPaths(@Nonnull final EntitySpec entitySpec) {
    Map<String, String> fieldPaths = new HashMap<>();

    entitySpec
        .getAspectSpecs()
        .forEach(
            aspectSpec -> {
              String aspectName = aspectSpec.getName();

              // Only collect paths for non-structuredProperties aspects and non-MAP_ARRAY fields
              if (!"structuredProperties".equals(aspectName)) {
                aspectSpec
                    .getSearchableFieldSpecs()
                    .forEach(
                        searchableFieldSpec -> {
                          FieldType fieldType =
                              searchableFieldSpec.getSearchableAnnotation().getFieldType();

                          // don't get root-level aliases
                          if (OBJECT_FIELD_TYPES.contains(fieldType)) {
                            return;
                          }

                          String fieldName =
                              searchableFieldSpec.getSearchableAnnotation().getFieldName();

                          String aspectFieldPath = "_aspects." + aspectName + "." + fieldName;
                          fieldPaths.put(fieldName, aspectFieldPath);
                        });
              }
            });

    return fieldPaths;
  }

  /**
   * Creates root-level fields based on field name conflicts. For fields with conflicts, creates a
   * root field that aspect fields copy_to. For fields without conflicts, creates a root alias
   * pointing to the single aspect field.
   *
   * @param entitySpecs all entity specs
   * @param fieldNameConflicts map of field names that have conflicts
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts
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

  /** Collects all field paths from all entity specs. */
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

  /** Collects all field name alias paths from all entity specs. */
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

  /** Creates root fields for regular field names. */
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

  /** Creates root fields for field name aliases. */
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

  /** Creates a mapping for a field that has conflicts (multiple aspects with same field name). */
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
        // Re-throw the exception for non-resolvable conflicts
        throw e;
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

  /** Creates an alias for a field that has no conflicts (single aspect with field name). */
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

  /** Creates a mapping for a field name alias that has conflicts. */
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

  /** Creates an alias for a field name alias that has no conflicts. */
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

  public static Map<String, Object> getMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec, @Nonnull final String aspectName) {
    return getMappingsForField(searchableFieldSpec, aspectName, Collections.emptyMap(), null);
  }

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
    entitySpec
        .getSearchableRefFieldSpecs()
        .forEach(
            entitySearchableRefFieldSpec ->
                mappingForField.putAll(
                    getMappingForSearchableRefField(
                        entityRegistry,
                        entitySearchableRefFieldSpec,
                        Math.min(
                            depth - 1,
                            entitySearchableRefFieldSpec
                                .getSearchableRefAnnotation()
                                .getDepth()))));

    mappingForField.put("urn", FieldTypeMapper.getMappingsForUrn());
    mappingForProperty.put("properties", mappingForField);

    mappings.put(baseFieldName, mappingForProperty);
    return mappings;
  }
}
