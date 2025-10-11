package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.utils.ESUtils.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.PATH;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Builder class for creating aspect-based field mappings. Handles the creation of mappings for
 * searchable fields within aspects.
 */
@Slf4j
public class AspectMappingBuilder {

  /**
   * Creates mappings for all aspects in an entity spec.
   *
   * @param entitySpec the entity spec to process
   * @param fieldNameConflicts map of field names that have conflicts
   * @return map of aspect mappings
   */
  public static Map<String, Object> createAspectMappings(
      @Nonnull EntitySpec entitySpec,
      @Nullable Map<String, Set<String>> fieldNameConflicts,
      @Nullable Map<String, Set<String>> fieldNameAliasConflicts) {

    Map<String, Object> aspectsMappings = new HashMap<>();

    // Process searchable fields - they will be grouped under _aspects.aspectName
    // Exception: structuredProperties aspect remains at root level
    entitySpec
        .getAspectSpecs()
        .forEach(
            aspectSpec -> {
              String aspectName = aspectSpec.getName();

              if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
                // Handle structuredProperties as a special case - keep at root level
                // This is handled separately by StructuredPropertyMappingBuilder
                return;
              }

              // Regular aspects go under _aspects
              Map<String, Object> aspectFields = new HashMap<>();

              aspectSpec
                  .getSearchableFieldSpecs()
                  .forEach(
                      searchableFieldSpec -> {
                        aspectFields.putAll(
                            MultiEntityMappingsBuilder.getMappingsForField(
                                searchableFieldSpec,
                                aspectName,
                                fieldNameConflicts,
                                fieldNameAliasConflicts));
                      });

              // Add _systemMetadata field to each aspect
              aspectFields.put("_systemMetadata", createSystemMetadataMapping());

              if (!aspectFields.isEmpty()) {
                aspectsMappings.put(aspectName, ImmutableMap.of(PROPERTIES, aspectFields));
                // Note: Logging is handled by the caller
              }
            });

    return aspectsMappings;
  }

  /**
   * Creates root-level aliases for field name aliases that don't have conflicts.
   *
   * @param entitySpec the entity spec to process
   * @param fieldNameAliasConflicts map of field name aliases that have conflicts
   * @return map of root-level aliases
   */
  public static Map<String, Object> createRootLevelAliases(
      @Nonnull EntitySpec entitySpec, @Nullable Map<String, Set<String>> fieldNameAliasConflicts) {

    Map<String, Object> rootAliases = new HashMap<>();

    entitySpec
        .getAspectSpecs()
        .forEach(
            aspectSpec -> {
              String aspectName = aspectSpec.getName();

              // Skip structuredProperties aspect - handled separately
              if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName)) {
                return;
              }

              aspectSpec
                  .getSearchableFieldSpecs()
                  .forEach(
                      searchableFieldSpec -> {
                        String baseFieldName =
                            searchableFieldSpec.getSearchableAnnotation().getFieldName();

                        // Skip OBJECT field types that are not MAP fields - aliases cannot point to
                        // dynamic object fields in Elasticsearch, but we can create aliases for
                        // MAP fields that were converted from OBJECT to MAP_ARRAY
                        FieldType fieldType =
                            searchableFieldSpec.getSearchableAnnotation().getFieldType();
                        if (OBJECT_FIELD_TYPES.contains(fieldType)) {
                          // Check if this is a MAP field by examining the underlying PDL schema
                          DataSchema currentSchema = searchableFieldSpec.getPegasusSchema();
                          if (currentSchema.getDereferencedType() != DataSchema.Type.MAP) {
                            // Skip non-MAP fields with OBJECT field types
                            return;
                          }
                          // For MAP fields with OBJECT field types, we can create aliases
                        }

                        String aspectFieldPath =
                            MappingConstants.ASPECTS_FIELD_NAME
                                + "."
                                + aspectName
                                + "."
                                + baseFieldName;

                        // Create aliases for field name aliases that don't have conflicts
                        if (searchableFieldSpec.getSearchableAnnotation().getFieldNameAliases()
                            != null) {
                          searchableFieldSpec
                              .getSearchableAnnotation()
                              .getFieldNameAliases()
                              .forEach(
                                  alias -> {
                                    if (fieldNameAliasConflicts == null
                                        || !fieldNameAliasConflicts.containsKey(alias)) {
                                      Map<String, Object> aliasMapping = new HashMap<>();
                                      aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);

                                      // Special handling for _entityName alias - point to
                                      // _search.entityName
                                      if (MultiEntityMappingsUtils.isEntityNameField(alias)
                                          && searchableFieldSpec
                                              .getSearchableAnnotation()
                                              .getSearchLabel()
                                              .isPresent()
                                          && "entityName"
                                              .equals(
                                                  searchableFieldSpec
                                                      .getSearchableAnnotation()
                                                      .getSearchLabel()
                                                      .get())) {
                                        aliasMapping.putAll(
                                            MultiEntityMappingsUtils
                                                .createEntityNameAliasMapping());
                                      } else {
                                        aliasMapping.put(PATH, aspectFieldPath);
                                      }

                                      rootAliases.put(alias, aliasMapping);
                                    }
                                    // Conflicted field name aliases are handled in
                                    // createRootFieldsWithCopyTo
                                  });
                        }

                        // Create aliases for hasValuesFieldName and numValuesFieldName fields
                        // These fields are created directly in the aspect, not nested under the
                        // array field
                        searchableFieldSpec
                            .getSearchableAnnotation()
                            .getHasValuesFieldName()
                            .ifPresent(
                                hasValuesFieldName -> {
                                  if (fieldNameAliasConflicts == null
                                      || !fieldNameAliasConflicts.containsKey(hasValuesFieldName)) {
                                    Map<String, Object> aliasMapping = new HashMap<>();
                                    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
                                    aliasMapping.put(
                                        PATH,
                                        MappingConstants.ASPECTS_FIELD_NAME
                                            + "."
                                            + aspectName
                                            + "."
                                            + hasValuesFieldName);
                                    rootAliases.put(hasValuesFieldName, aliasMapping);
                                  }
                                });

                        searchableFieldSpec
                            .getSearchableAnnotation()
                            .getNumValuesFieldName()
                            .ifPresent(
                                numValuesFieldName -> {
                                  if (fieldNameAliasConflicts == null
                                      || !fieldNameAliasConflicts.containsKey(numValuesFieldName)) {
                                    Map<String, Object> aliasMapping = new HashMap<>();
                                    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
                                    aliasMapping.put(
                                        PATH,
                                        MappingConstants.ASPECTS_FIELD_NAME
                                            + "."
                                            + aspectName
                                            + "."
                                            + numValuesFieldName);
                                    rootAliases.put(numValuesFieldName, aliasMapping);
                                  }
                                });
                      });
            });

    return rootAliases;
  }

  /**
   * Creates the mapping structure for the _systemMetadata field that appears in each aspect. This
   * replaces the need for a dynamic template by explicitly defining the structure.
   *
   * @return mapping configuration for _systemMetadata field
   */
  private static Map<String, Object> createSystemMetadataMapping() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, "object");

    Map<String, Object> properties = new HashMap<>();

    // lastObserved field
    Map<String, Object> lastObserved = new HashMap<>();
    lastObserved.put(TYPE, "date");
    lastObserved.put("copy_to", new String[] {"_search._system_lastObserved"});
    properties.put("lastObserved", lastObserved);

    // runId field
    Map<String, Object> runId = new HashMap<>();
    runId.put(TYPE, "keyword");
    runId.put("copy_to", new String[] {"_search._system_runId"});
    properties.put("runId", runId);

    // lastRunId field
    Map<String, Object> lastRunId = new HashMap<>();
    lastRunId.put(TYPE, "keyword");
    lastRunId.put("copy_to", new String[] {"_search._system_lastRunId"});
    properties.put("lastRunId", lastRunId);

    // aspectCreated field
    Map<String, Object> aspectCreated = new HashMap<>();
    aspectCreated.put(TYPE, "object");
    Map<String, Object> aspectCreatedProperties = new HashMap<>();

    Map<String, Object> aspectCreatedTime = new HashMap<>();
    aspectCreatedTime.put(TYPE, "date");
    aspectCreatedTime.put("copy_to", new String[] {"_search._system_aspectCreated_time"});
    aspectCreatedProperties.put("time", aspectCreatedTime);

    Map<String, Object> aspectCreatedActor = new HashMap<>();
    aspectCreatedActor.put(TYPE, "keyword");
    aspectCreatedActor.put("copy_to", new String[] {"_search._system_aspectCreated_actor"});
    aspectCreatedProperties.put("actor", aspectCreatedActor);

    Map<String, Object> aspectCreatedImpersonator = new HashMap<>();
    aspectCreatedImpersonator.put(TYPE, "keyword");
    aspectCreatedProperties.put("impersonator", aspectCreatedImpersonator);

    aspectCreated.put(PROPERTIES, aspectCreatedProperties);
    properties.put("aspectCreated", aspectCreated);

    // aspectModified field
    Map<String, Object> aspectModified = new HashMap<>();
    aspectModified.put(TYPE, "object");
    Map<String, Object> aspectModifiedProperties = new HashMap<>();

    Map<String, Object> aspectModifiedTime = new HashMap<>();
    aspectModifiedTime.put(TYPE, "date");
    aspectModifiedTime.put("copy_to", new String[] {"_search._system_aspectModified_time"});
    aspectModifiedProperties.put("time", aspectModifiedTime);

    Map<String, Object> aspectModifiedActor = new HashMap<>();
    aspectModifiedActor.put(TYPE, "keyword");
    aspectModifiedActor.put("copy_to", new String[] {"_search._system_aspectModified_actor"});
    aspectModifiedProperties.put("actor", aspectModifiedActor);

    Map<String, Object> aspectModifiedImpersonator = new HashMap<>();
    aspectModifiedImpersonator.put(TYPE, "keyword");
    aspectModifiedProperties.put("impersonator", aspectModifiedImpersonator);

    aspectModified.put(PROPERTIES, aspectModifiedProperties);
    properties.put("aspectModified", aspectModified);

    mapping.put(PROPERTIES, properties);
    return mapping;
  }
}
