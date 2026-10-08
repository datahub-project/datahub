package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getEntityTypeId;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
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
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
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
import java.util.SortedSet;
import java.util.TreeSet;
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
 *   <li>Real root-level projected fields for the V2.5-compatible search surface
 *   <li>Root-level projected fields own copy_to into the {@code _search} aggregate fields
 *   <li>Structured properties support under {@code structuredProperties} field
 *   <li>Search label organization under {@code _search} object
 * </ul>
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Conflict resolution for fields appearing in multiple aspects
 *   <li>Type conflict resolution using configurable strategies
 *   <li>Support for field name aliases
 *   <li>Dynamic structured properties handling
 *   <li>Search label organization
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
 *             "owners": { "type": "keyword" }
 *           }
 *         }
 *       }
 *     },
 *     "owners": { "type": "keyword" },
 *     "_search": {
 *       "properties": {
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
  @Nullable private final Map<String, Object> mappingBaseConfiguration;

  private final int keywordMaxLength;

  /** Engine-specific search-as-you-type shape of the {@code _search.autocomplete} ngram field. */
  @Nonnull private final Map<String, String> partialNgramConfig;

  @Nonnull private final List<V3MappingContributor> mappingContributors;

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
    this(entityIndexConfiguration, null, ESUtils.KEYWORD_MAXLENGTH, List.of());
  }

  public MultiEntityMappingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nullable SearchClientShim<?> searchClientShim)
      throws IOException {
    this(entityIndexConfiguration, searchClientShim, ESUtils.KEYWORD_MAXLENGTH, List.of());
  }

  public MultiEntityMappingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration, int keywordMaxLength)
      throws IOException {
    this(entityIndexConfiguration, null, keywordMaxLength, List.of());
  }

  public MultiEntityMappingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      int keywordMaxLength,
      @Nonnull List<V3MappingContributor> mappingContributors)
      throws IOException {
    this(entityIndexConfiguration, null, keywordMaxLength, mappingContributors);
  }

  public MultiEntityMappingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nullable SearchClientShim<?> searchClientShim,
      int keywordMaxLength)
      throws IOException {
    this(entityIndexConfiguration, searchClientShim, keywordMaxLength, List.of());
  }

  public MultiEntityMappingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nullable SearchClientShim<?> searchClientShim,
      int keywordMaxLength,
      @Nonnull List<V3MappingContributor> mappingContributors)
      throws IOException {

    this.entityIndexConfiguration = entityIndexConfiguration;
    this.partialNgramConfig =
        searchClientShim != null
            ? searchClientShim.partialNgramConfig()
            : FieldTypeMapper.DEFAULT_PARTIAL_NGRAM_CONFIG;
    this.keywordMaxLength = keywordMaxLength > 0 ? keywordMaxLength : ESUtils.KEYWORD_MAXLENGTH;
    this.mappingContributors =
        mappingContributors == null ? List.of() : List.copyOf(mappingContributors);
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
      // Generate one index mapping per V3 index key (explicit searchGroup, else unset fallback)
      return V3IndexKeys.groupEntitySpecs(opContext.getEntityRegistry()).keySet().stream()
          .map(
              indexKey -> {
                Map<String, Object> mappings =
                    getMappingsForMultipleEntities(
                        opContext.getEntityRegistry(), indexKey, structuredProperties);
                return IndexMapping.builder()
                    .indexName(
                        opContext
                            .getSearchContext()
                            .getIndexConvention()
                            .getEntityIndexNameV3(opContext, indexKey))
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

      if (entitySpec != null) {
        String indexKey = V3IndexKeys.resolve(entitySpec);
        searchGroupToEntitySpecs.computeIfAbsent(indexKey, k -> new ArrayList<>()).add(entitySpec);
      } else {
        log.warn("Missing entitySpec for entity type: {}", entityTypeName);
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
                      .getEntityIndexNameV3(opContext, searchGroup))
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
    List<StructuredPropertyUtils.StructuredPropertyFieldMapping> entries =
        properties.stream()
            .map(
                urnProperty -> {
                  StructuredPropertyDefinition property = urnProperty.getSecond();
                  Map<String, Object> mappingForField = new HashMap<>();
                  LogicalValueType logicalType = getLogicalValueType(property.getValueType());

                  switch (logicalType) {
                    case STRING:
                    case RICH_TEXT:
                      mappingForField =
                          FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove(keywordMaxLength);
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
                      mappingForField =
                          FieldTypeMapper.getMappingsForKeywordWithIgnoreAbove(keywordMaxLength);
                      break;
                  }

                  return new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                      toElasticsearchFieldName(urnProperty.getFirst(), property),
                      urnProperty.getFirst(),
                      mappingForField);
                })
            .collect(Collectors.toList());
    return StructuredPropertyUtils.resolveStructuredPropertyMappingCollisions(entries);
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
   *   <li>Creates real root-level projected fields
   *   <li>Merges with base configuration if available
   *   <li>Builds the _search section for label organization
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
    Collection<EntitySpec> entitySpecs = V3IndexKeys.entitySpecsForKey(entityRegistry, searchGroup);

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

    Map<String, Object> rootProjectionFields =
        createRootProjectionFields(entitySpecs, fieldNameConflicts);
    if (!rootProjectionFields.isEmpty()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) combinedMappings.get("properties");
      if (properties != null) {
        properties.putAll(rootProjectionFields);
      }
    }

    // Merge with base configuration if available
    if (mappingBaseConfiguration != null) {
      combinedMappings =
          MultiEntityMappingsUtils.mergeMappings(combinedMappings, mappingBaseConfiguration);
    }

    applyGeneratedRootSystemMappings(combinedMappings);

    applyMappingContributors(combinedMappings, searchGroup);

    // Build _search section with all copy_to destination fields
    Map<String, Object> searchSection =
        MultiEntityMappingsUtils.buildSearchSection(
            entitySpecs, combinedMappings, partialNgramConfig);
    if (!searchSection.isEmpty()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) combinedMappings.get("properties");
      if (properties != null) {
        keepBaseSearchFields(properties.get("_search"), searchSection);
        properties.put("_search", searchSection);
      }
    }

    return combinedMappings;
  }

  /**
   * The base configuration types the {@code _search._system_*} fields that per-aspect system
   * metadata copies into; without them the engine maps those copies dynamically. Fields built from
   * the models' search labels take precedence.
   */
  @SuppressWarnings("unchecked")
  private static void keepBaseSearchFields(
      @Nullable final Object baseSearchSection, @Nonnull final Map<String, Object> searchSection) {
    if (!(baseSearchSection instanceof Map)
        || !(((Map<String, Object>) baseSearchSection).get(PROPERTIES) instanceof Map)) {
      return;
    }
    final Map<String, Object> merged =
        new HashMap<>(
            (Map<String, Object>) ((Map<String, Object>) baseSearchSection).get(PROPERTIES));
    merged.putAll((Map<String, Object>) searchSection.get(PROPERTIES));
    searchSection.put(PROPERTIES, merged);
  }

  @SuppressWarnings("unchecked")
  private void applyGeneratedRootSystemMappings(@Nonnull final Map<String, Object> mappings) {
    final Object propertiesObject = mappings.get(PROPERTIES);
    if (!(propertiesObject instanceof Map)) {
      return;
    }

    final Map<String, Object> properties = (Map<String, Object>) propertiesObject;
    final Object existingUrnMapping = properties.get("urn");
    final Map<String, Object> urnMapping =
        existingUrnMapping instanceof Map
            ? new HashMap<>((Map<String, Object>) existingUrnMapping)
            : new HashMap<>(Map.of(TYPE, ESUtils.KEYWORD_FIELD_TYPE));
    // Exact urn matches use the keyword; full-text search finds the urn's parts in _search.other,
    // as V2 searches the urn by default
    urnMapping.put(COPY_TO, List.of(V3SearchFields.path(V3SearchFields.OTHER)));
    properties.put("urn", urnMapping);
    // The projector writes _entityType into every V3 document; without an explicit mapping,
    // dynamic mapping makes it analyzed text and the entity-type facet aggregation (and exact
    // filters on camelCase entity names) fail on the consolidated index. No normalizer: the
    // entity-type facet includes the registry names case-sensitively.
    properties.putIfAbsent(
        V3SearchDocumentProjector.ENTITY_TYPE_FIELD, new HashMap<>(Map.of(TYPE, "keyword")));
  }

  private void applyMappingContributors(
      @Nonnull Map<String, Object> combinedMappings, @Nonnull String searchGroup) {
    if (mappingContributors.isEmpty()) {
      return;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) combinedMappings.get("properties");
    if (properties == null) {
      properties = new HashMap<>();
      combinedMappings.put("properties", properties);
    }
    for (V3MappingContributor contributor : mappingContributors) {
      Map<String, Object> extras = contributor.extraRootProperties(searchGroup);
      for (Map.Entry<String, Object> extra : extras.entrySet()) {
        if (properties.containsKey(extra.getKey())
            || MappingConstants.STRATEGY_OWNED_ROOT_FIELDS.contains(extra.getKey())) {
          throw new IllegalArgumentException(
              "V3 mapping contributor attempted to overwrite existing property '"
                  + extra.getKey()
                  + "'");
        }
        properties.put(extra.getKey(), extra.getValue());
      }
    }
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

    // The projector writes each reference field at the root, where V2 queries and filters read it,
    // and with the rest of its aspect under _aspects.<aspect>, where it stays unanalyzed
    final Map<String, Object> refFieldMappings = new HashMap<>();
    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      for (SearchableRefFieldSpec searchableRefFieldSpec :
          aspectSpec.getSearchableRefFieldSpecs()) {
        final int depth = searchableRefFieldSpec.getSearchableRefAnnotation().getDepth();
        refFieldMappings.putAll(
            getMappingForSearchableRefField(
                entityRegistry,
                searchableRefFieldSpec,
                depth,
                false,
                searchableRefFieldSpec.getSearchableRefAnnotation().isQueryByDefault()));
        // structuredProperties has no _aspects entry: the projector writes it at the root only
        final Object aspectMapping = finalAspectsMappings.get(aspectSpec.getName());
        if (aspectMapping instanceof Map) {
          @SuppressWarnings("unchecked")
          final Map<String, Object> aspectFields =
              new HashMap<>(
                  (Map<String, Object>) ((Map<String, Object>) aspectMapping).get(PROPERTIES));
          aspectFields.putAll(
              getMappingForSearchableRefField(
                  entityRegistry, searchableRefFieldSpec, depth, true, false));
          finalAspectsMappings.put(aspectSpec.getName(), ImmutableMap.of(PROPERTIES, aspectFields));
        }
      }
    }
    mappings.putAll(refFieldMappings);

    // Add _aspects object to root mappings
    if (!finalAspectsMappings.isEmpty()) {
      mappings.put(
          MappingConstants.ASPECTS_FIELD_NAME, ImmutableMap.of(PROPERTIES, finalAspectsMappings));
    }

    // Process structured properties using StructuredPropertyMappingBuilder
    Map<String, Object> structuredPropertyMappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            entitySpec, structuredProperties);

    // Add structured properties from parameters under a structuredProperties container.
    // dynamic:false — a structured property value indexed before its mapping update must stay
    // in _source unindexed rather than be dynamic-mapped as text, which permanently poisons the
    // field type and breaks terms aggregations across multi-index searches.
    mappings.put(
        STRUCTURED_PROPERTY_MAPPING_FIELD,
        ImmutableMap.of(
            TYPE,
            ESUtils.OBJECT_FIELD_TYPE,
            "dynamic",
            false,
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

      // Skip object field types from root projection unless handled through field aliases.
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
   * Creates real root-level projected fields, the keyword and typed fields filters, facets and
   * sorts read, each copying into the shared {@code _search} fields its source fields feed (see
   * {@link V3SearchFields}). Aspect fields remain under {@code _aspects}; they do not copy into
   * these root fields.
   */
  private static Map<String, Object> createRootProjectionFields(
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Map<String, Set<String>> fieldNameConflicts) {

    Map<String, Object> rootFields = new HashMap<>();

    // Collect all field paths from all entities
    Map<String, Set<String>> fieldNameToAllPaths = collectAllFieldPaths(entitySpecs);
    Map<String, Set<String>> fieldNameAliasToAllPaths = collectAllFieldNameAliasPaths(entitySpecs);
    Set<SearchableFieldSpec> entityNameFallbacks = V3SearchFields.entityNameFallbacks(entitySpecs);

    createRootFieldsForFieldNames(
        rootFields, fieldNameToAllPaths, entitySpecs, entityNameFallbacks);

    createRootFieldsForFieldNameAliases(
        rootFields, fieldNameAliasToAllPaths, fieldNameConflicts, entitySpecs);

    createDerivedRootProjectionFields(rootFields, entitySpecs);

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
   * Creates real root fields for regular field names based on conflict analysis.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldNameToAllPaths map of field names to all their paths
   * @param entitySpecs all entity specifications for type resolution
   * @param entityNameFallbacks fields that feed {@code _search.entityName} without naming it
   */
  private static void createRootFieldsForFieldNames(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull Map<String, Set<String>> fieldNameToAllPaths,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchableFieldSpec> entityNameFallbacks) {

    final Map<String, List<SearchableFieldSpec>> sharedFieldSources =
        V3SearchFields.rootSourceFieldSpecs(entitySpecs);
    for (Map.Entry<String, Set<String>> entry : fieldNameToAllPaths.entrySet()) {
      String fieldName = entry.getKey();
      Set<String> allPaths = entry.getValue();

      createProjectedRootFieldMapping(
          rootFields,
          fieldName,
          allPaths,
          entitySpecs,
          entityNameFallbacks,
          sharedFieldSources.getOrDefault(fieldName, List.of()));
    }
  }

  /**
   * Creates root fields for field name aliases based on conflict analysis. Handles both conflicted
   * and non-conflicted aliases, with special handling for aliases that conflict with regular field
   * names.
   *
   * @param rootFields map to populate with root field configurations
   * @param fieldNameAliasToAllPaths map of field name aliases to all their paths
   * @param fieldNameConflicts map of field names that have conflicts (for overlap detection)
   * @param entitySpecs all entity specifications for type resolution
   */
  private static void createRootFieldsForFieldNameAliases(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull Map<String, Set<String>> fieldNameAliasToAllPaths,
      @Nonnull Map<String, Set<String>> fieldNameConflicts,
      @Nonnull Collection<EntitySpec> entitySpecs) {

    for (Map.Entry<String, Set<String>> entry : fieldNameAliasToAllPaths.entrySet()) {
      String alias = entry.getKey();
      Set<String> allPaths = entry.getValue();

      if (fieldNameConflicts.containsKey(alias) || rootFields.containsKey(alias)) {
        log.debug(
            "Skipping field name alias '{}' as it conflicts with a field name already projected",
            alias);
        continue;
      }

      if (MultiEntityMappingsUtils.isEntityNameField(alias)) {
        // _entityName must stay an Elasticsearch field alias to a populated field so the term
        // suggester (ESUtils.buildNameSuggestions) resolves it. A concrete projected root field
        // here is never populated (nothing copies into it and the projector does not write it),
        // which silently breaks V3 name suggestions.
        rootFields.put(alias, createEntityNameAliasMapping(alias, allPaths, entitySpecs));
        continue;
      }

      // As on V2, an alias points at the root field it names: documents hold each value under the
      // field's own name, so a separate root field under the alias would stay empty
      final String aliasedField = aliasedFieldName(alias, allPaths);
      if (rootFields.containsKey(aliasedField)) {
        rootFields.put(alias, MultiEntityMappingsUtils.createAliasMapping(aliasedField));
        continue;
      }

      // Only an object field has no root field, and an engine alias cannot point at an object
      log.warn(
          "Field name alias '{}' names '{}', which has no root field, so filters on the alias match"
              + " nothing",
          alias,
          aliasedField);
      createProjectedRootFieldMapping(
          rootFields, alias, allPaths, entitySpecs, Set.of(), List.of());
    }
  }

  /**
   * Aliases {@code _entityName} to {@code _search.entityName} when a root field of the index copies
   * into it. Otherwise the alias points at the root field it names, as on V2: with one index per
   * entity, {@code _search.entityName} is only mapped when a field feeds it, and an alias to an
   * unmapped field makes the index mapping invalid.
   */
  private static Map<String, Object> createEntityNameAliasMapping(
      @Nonnull String alias,
      @Nonnull Set<String> allPaths,
      @Nonnull Collection<EntitySpec> entitySpecs) {
    if (V3SearchFields.isFed(entitySpecs, V3SearchFields.ENTITY_NAME)) {
      return MultiEntityMappingsUtils.createEntityNameAliasMapping();
    }
    return MultiEntityMappingsUtils.createAliasMapping(aliasedFieldName(alias, allPaths));
  }

  /**
   * The field an alias names, from its {@code _aspects.<aspect>.<field>} paths. When entities of
   * the index alias different fields, the first in order is used, as V2 keeps one of them too
   * rather than failing every index build.
   */
  private static String aliasedFieldName(@Nonnull String alias, @Nonnull Set<String> allPaths) {
    final SortedSet<String> fieldNames =
        allPaths.stream()
            .map(path -> path.split("\\.", 3))
            .filter(pathParts -> pathParts.length == 3)
            .map(pathParts -> pathParts[2])
            .collect(Collectors.toCollection(TreeSet::new));
    if (fieldNames.size() > 1) {
      log.warn("'{}' aliases the fields {}; aliasing '{}'", alias, fieldNames, fieldNames.first());
    }
    return fieldNames.first();
  }

  private static void createDerivedRootProjectionFields(
      @Nonnull Map<String, Object> rootFields, @Nonnull Collection<EntitySpec> entitySpecs) {
    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName())) {
          continue;
        }
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          fieldSpec
              .getSearchableAnnotation()
              .getHasValuesFieldName()
              .ifPresent(
                  fieldName ->
                      rootFields.putIfAbsent(
                          fieldName, ImmutableMap.of(TYPE, ESUtils.BOOLEAN_FIELD_TYPE)));
          fieldSpec
              .getSearchableAnnotation()
              .getNumValuesFieldName()
              .ifPresent(
                  fieldName ->
                      rootFields.putIfAbsent(
                          fieldName, ImmutableMap.of(TYPE, ESUtils.LONG_FIELD_TYPE)));
          ESUtils.getSystemModifiedAtFieldName(fieldSpec)
              .ifPresent(
                  fieldName ->
                      rootFields.putIfAbsent(
                          fieldName, ImmutableMap.of(TYPE, ESUtils.DATE_FIELD_TYPE)));
        }
      }
    }
  }

  /**
   * @param entityNameFallbacks fields that feed {@code _search.entityName} without naming it
   * @param sharedFieldSources every source field of the root field, whose shared {@code _search}
   *     fields it copies into (see {@link V3SearchFields#rootSourceFieldSpecs}); none for an alias
   */
  private static void createProjectedRootFieldMapping(
      @Nonnull Map<String, Object> rootFields,
      @Nonnull String rootFieldName,
      @Nonnull Set<String> allPaths,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchableFieldSpec> entityNameFallbacks,
      @Nonnull List<SearchableFieldSpec> sharedFieldSources) {

    final List<SearchableFieldSpec> sourceFieldSpecs =
        findSearchableFieldSpecsForPaths(entitySpecs, allPaths);
    final Map<String, Object> rootFieldMapping =
        resolveProjectedRootFieldMapping(rootFieldName, sourceFieldSpecs);

    addSearchCopyToDestinations(
        rootFieldMapping, sharedFieldSources, rootFieldName, entityNameFallbacks);

    rootFields.put(rootFieldName, rootFieldMapping);
    log.debug("Creating real root projection field '{}'", rootFieldName);
  }

  private static List<SearchableFieldSpec> findSearchableFieldSpecsForPaths(
      @Nonnull Collection<EntitySpec> entitySpecs, @Nonnull Set<String> allPaths) {
    final List<SearchableFieldSpec> sourceFieldSpecs = new ArrayList<>();

    for (String path : allPaths) {
      final String[] pathParts = path.split("\\.", 3);
      if (pathParts.length < 3) {
        log.warn("Skipping malformed V3 projected root field path '{}'", path);
        continue;
      }

      final String aspectName = pathParts[1];
      final String fieldName = pathParts[2];
      for (EntitySpec entitySpec : entitySpecs) {
        for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
          if (!aspectName.equals(aspectSpec.getName())) {
            continue;
          }
          for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
            if (fieldName.equals(fieldSpec.getSearchableAnnotation().getFieldName())) {
              sourceFieldSpecs.add(fieldSpec);
            }
          }
        }
      }
    }

    return sourceFieldSpecs;
  }

  private static Map<String, Object> resolveProjectedRootFieldMapping(
      @Nonnull String rootFieldName, @Nonnull List<SearchableFieldSpec> sourceFieldSpecs) {
    if (sourceFieldSpecs.isEmpty()) {
      log.warn(
          "Could not find source fields for V3 root projection '{}', defaulting to keyword",
          rootFieldName);
      return new HashMap<>(FieldTypeMapper.getMappingsForKeyword());
    }

    final Set<String> elasticsearchTypes =
        sourceFieldSpecs.stream()
            .map(
                fieldSpec ->
                    FieldTypeMapper.getElasticsearchTypeForFieldType(
                        fieldSpec.getSearchableAnnotation().getFieldType(), fieldSpec))
            .collect(Collectors.toSet());

    final String resolvedElasticsearchType;
    try {
      resolvedElasticsearchType = ConflictResolver.resolveTypeConflict(elasticsearchTypes);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "Non-resolvable field type conflict for projected root field '%s' with types %s. %s",
              rootFieldName, elasticsearchTypes, e.getMessage()),
          e);
    }
    // The field is mapped as its source fields of the resolved type are, subfields included
    final List<SearchableFieldSpec> resolvedFieldSpecs =
        sourceFieldSpecs.stream()
            .filter(
                fieldSpec ->
                    resolvedElasticsearchType.equals(
                        FieldTypeMapper.getElasticsearchTypeForFieldType(
                            fieldSpec.getSearchableAnnotation().getFieldType(), fieldSpec)))
            .collect(Collectors.toList());
    final Map<String, Object> rootFieldMapping =
        new HashMap<>(FieldTypeMapper.getRichestCompatibleMapping(resolvedFieldSpecs));

    applyProjectedRootFieldOptions(rootFieldMapping, sourceFieldSpecs);
    return rootFieldMapping;
  }

  private static void applyProjectedRootFieldOptions(
      @Nonnull Map<String, Object> rootFieldMapping,
      @Nonnull List<SearchableFieldSpec> sourceFieldSpecs) {
    final boolean hasEagerGlobalOrdinals =
        sourceFieldSpecs.stream()
            .anyMatch(
                fieldSpec ->
                    fieldSpec.getSearchableAnnotation().getEagerGlobalOrdinals().orElse(false)
                        && isEagerGlobalOrdinalsSupported(
                            fieldSpec.getSearchableAnnotation().getFieldType()));
    if (hasEagerGlobalOrdinals) {
      putEagerGlobalOrdinals(rootFieldMapping);
    }
  }

  /**
   * Turns on eager global ordinals on the {@code .keyword} subfield that filters and facets read,
   * or on the field itself when it has none.
   */
  @SuppressWarnings("unchecked")
  private static void putEagerGlobalOrdinals(@Nonnull Map<String, Object> fieldMapping) {
    if (fieldMapping.get(ESUtils.FIELDS) instanceof Map<?, ?> subfields
        && subfields.get(ESUtils.KEYWORD) instanceof Map<?, ?> keyword) {
      Map<String, Object> keywordMapping = new HashMap<>((Map<String, Object>) keyword);
      keywordMapping.put("eager_global_ordinals", true);
      Map<String, Object> subfieldMappings = new HashMap<>((Map<String, Object>) subfields);
      subfieldMappings.put(ESUtils.KEYWORD, keywordMapping);
      fieldMapping.put(ESUtils.FIELDS, subfieldMappings);
      return;
    }
    fieldMapping.put("eager_global_ordinals", true);
  }

  private static boolean isEagerGlobalOrdinalsSupported(@Nonnull final FieldType fieldType) {
    return fieldType == FieldType.KEYWORD
        || fieldType == FieldType.URN
        || fieldType == FieldType.URN_PARTIAL;
  }

  private static void addSearchCopyToDestinations(
      @Nonnull Map<String, Object> rootFieldMapping,
      @Nonnull List<SearchableFieldSpec> sourceFieldSpecs,
      @Nonnull String rootFieldName,
      @Nonnull Set<SearchableFieldSpec> entityNameFallbacks) {
    final List<String> copyToDestinations =
        V3SearchFields.destinations(sourceFieldSpecs, entityNameFallbacks).stream()
            .map(V3SearchFields::path)
            .collect(Collectors.toList());

    if (!copyToDestinations.isEmpty()) {
      rootFieldMapping.put(COPY_TO, copyToDestinations);
      log.debug(
          "Adding _search copy_to destinations for root projection field '{}': {}",
          rootFieldName,
          copyToDestinations);
    }
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
    return getMappingsForField(searchableFieldSpec, aspectName, true);
  }

  /**
   * Gets Elasticsearch mappings for a single searchable field specification. This method creates
   * comprehensive field mappings including:
   *
   * <ul>
   *   <li>Field type mapping based on annotations
   *   <li>Eager global ordinals configuration
   *   <li>Optional search label and entity field name copy_to fields
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
    return getMappingsForField(searchableFieldSpec, aspectName, true);
  }

  public static Map<String, Object> getMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec,
      @Nonnull final String aspectName,
      final boolean includeSearchCopyTo) {
    // Use the enhanced mapping that considers the underlying PDL field type for more precise
    // numeric types
    return buildFieldMappings(
        searchableFieldSpec,
        aspectName,
        includeSearchCopyTo,
        FieldTypeMapper.getMappingsForFieldType(
            searchableFieldSpec.getSearchableAnnotation().getFieldType(), searchableFieldSpec));
  }

  /**
   * Gets the mappings for a field's copy under {@code _aspects.<aspect>}, which stays unanalyzed
   * (see {@link FieldTypeMapper#getAspectMappingsForFieldType}) and copies into no {@code _search}
   * field.
   */
  public static Map<String, Object> getAspectMappingsForField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec, @Nonnull final String aspectName) {
    return buildFieldMappings(
        searchableFieldSpec,
        aspectName,
        false,
        FieldTypeMapper.getAspectMappingsForFieldType(
            searchableFieldSpec.getSearchableAnnotation().getFieldType(), searchableFieldSpec));
  }

  private static Map<String, Object> buildFieldMappings(
      @Nonnull final SearchableFieldSpec searchableFieldSpec,
      @Nonnull final String aspectName,
      final boolean includeSearchCopyTo,
      @Nonnull final Map<String, Object> fieldTypeMapping) {
    FieldType fieldType = searchableFieldSpec.getSearchableAnnotation().getFieldType();
    String baseFieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();

    String actualFieldName = baseFieldName;

    log.debug(
        "Processing field '{}' of type '{}' for aspect '{}'", baseFieldName, fieldType, aspectName);

    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>(fieldTypeMapping);

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
                  putEagerGlobalOrdinals(mappingForField);
                  log.debug("Setting eager_global_ordinals=true for field '{}'", baseFieldName);
                } else {
                  log.debug(
                      "Skipping eager_global_ordinals for field '{}' with field type '{}' (not supported)",
                      baseFieldName,
                      fieldType);
                }
              }
            });

    if (includeSearchCopyTo) {
      addSearchCopyToDestinations(
          mappingForField, List.of(searchableFieldSpec), baseFieldName, Set.of());
    }

    applyProjectedRootFieldOptions(mappingForField, List.of(searchableFieldSpec));

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
   * @param aspectCopy whether the mapping is for the unanalyzed copy under {@code _aspects}
   * @param queriedByDefault whether full-text search reads the reference, as V2 does when every
   *     reference on the way to it is queried by default
   * @return map containing the reference field mapping configuration
   */
  private static Map<String, Object> getMappingForSearchableRefField(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull final SearchableRefFieldSpec searchableRefFieldSpec,
      @Nonnull final int depth,
      final boolean aspectCopy,
      final boolean queriedByDefault) {
    Map<String, Object> mappings = new HashMap<>();
    Map<String, Object> mappingForField = new HashMap<>();
    Map<String, Object> mappingForProperty = new HashMap<>();

    String baseFieldName = searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName();

    final Map<String, Object> urnMapping;
    if (aspectCopy) {
      urnMapping = FieldTypeMapper.getMappingsForUrn();
    } else if (queriedByDefault) {
      // V2 searches the urn of every reference it searches
      urnMapping =
          Map.of(
              TYPE,
              ESUtils.KEYWORD_FIELD_TYPE,
              COPY_TO,
              List.of(V3SearchFields.path(V3SearchFields.OTHER)));
    } else {
      urnMapping = Map.of(TYPE, ESUtils.KEYWORD_FIELD_TYPE);
    }
    if (depth == 0) {
      mappings.put(baseFieldName, urnMapping);
      return mappings;
    }

    String entityType = searchableRefFieldSpec.getSearchableRefAnnotation().getRefType();
    EntitySpec entitySpec = entityRegistry.getEntitySpec(entityType);

    entitySpec
        .getSearchableFieldSpecs()
        .forEach(
            searchableFieldSpec ->
                mappingForField.putAll(
                    aspectCopy
                        ? getAspectMappingsForField(searchableFieldSpec, "ref_" + entityType)
                        : getMappingsForReferencedField(
                            searchableFieldSpec, entityType, queriedByDefault)));
    // Process searchable reference fields recursively
    for (SearchableRefFieldSpec refFieldSpec : entitySpec.getSearchableRefFieldSpecs()) {
      int configuredDepth = refFieldSpec.getSearchableRefAnnotation().getDepth();
      int remainingDepth = Math.min(depth - 1, configuredDepth);

      Map<String, Object> refFieldMappings =
          getMappingForSearchableRefField(
              entityRegistry,
              refFieldSpec,
              remainingDepth,
              aspectCopy,
              queriedByDefault && refFieldSpec.getSearchableRefAnnotation().isQueryByDefault());

      mappingForField.putAll(refFieldMappings);
    }

    mappingForField.put("urn", urnMapping);
    mappingForProperty.put("properties", mappingForField);

    mappings.put(baseFieldName, mappingForProperty);
    return mappings;
  }

  /**
   * A referenced entity's field under a reference field. When full-text search reads the reference,
   * its string values queried by default copy into {@code _search.other}, as V2 searches them at
   * the reference's boost; never into the shared fields that name the referencing entity.
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> getMappingsForReferencedField(
      @Nonnull final SearchableFieldSpec searchableFieldSpec,
      @Nonnull final String entityType,
      final boolean queriedByDefault) {
    final Map<String, Object> mappings =
        getMappingsForField(searchableFieldSpec, "ref_" + entityType, false);
    final SearchableAnnotation annotation = searchableFieldSpec.getSearchableAnnotation();
    if (queriedByDefault
        && annotation.isQueryByDefault()
        && V3SearchFields.isStringFieldType(annotation.getFieldType())
        && mappings.get(annotation.getFieldName()) instanceof Map<?, ?> fieldMapping) {
      final Map<String, Object> withCopyTo = new HashMap<>((Map<String, Object>) fieldMapping);
      withCopyTo.put(COPY_TO, List.of(V3SearchFields.path(V3SearchFields.OTHER)));
      mappings.put(annotation.getFieldName(), withCopyTo);
    }
    return mappings;
  }
}
