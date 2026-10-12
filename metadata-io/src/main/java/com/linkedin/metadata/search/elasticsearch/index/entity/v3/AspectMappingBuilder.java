package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import java.util.HashMap;
import java.util.List;
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
    entitySpec.getAspectSpecs().stream()
        .filter(aspectSpec -> !STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()))
        .forEach(
            aspectSpec -> {
              String aspectName = aspectSpec.getName();

              // Regular aspects go under _aspects
              Map<String, Object> aspectFields = new HashMap<>();

              aspectSpec
                  .getSearchableFieldSpecs()
                  .forEach(
                      searchableFieldSpec -> {
                        aspectFields.putAll(
                            MultiEntityMappingsBuilder.getAspectMappingsForField(
                                searchableFieldSpec, aspectName));
                      });

              // Add system metadata to each aspect using the projector's serialized field name.
              aspectFields.put(
                  V3SearchDocumentProjector.SYSTEM_METADATA_FIELD, createSystemMetadataMapping());

              if (!aspectFields.isEmpty()) {
                aspectsMappings.put(aspectName, ImmutableMap.of(PROPERTIES, aspectFields));
              }
            });

    return aspectsMappings;
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
    lastObserved.put("copy_to", List.of("_search._system_lastObserved"));
    properties.put("lastObserved", lastObserved);

    // runId field
    Map<String, Object> runId = new HashMap<>();
    runId.put(TYPE, "keyword");
    runId.put("copy_to", List.of("_search._system_runId"));
    properties.put("runId", runId);

    // lastRunId field
    Map<String, Object> lastRunId = new HashMap<>();
    lastRunId.put(TYPE, "keyword");
    lastRunId.put("copy_to", List.of("_search._system_lastRunId"));
    properties.put("lastRunId", lastRunId);

    // aspectCreated field
    Map<String, Object> aspectCreated = new HashMap<>();
    aspectCreated.put(TYPE, "object");
    Map<String, Object> aspectCreatedProperties = new HashMap<>();

    Map<String, Object> aspectCreatedTime = new HashMap<>();
    aspectCreatedTime.put(TYPE, "date");
    aspectCreatedTime.put("copy_to", List.of("_search._system_aspectCreated_time"));
    aspectCreatedProperties.put("time", aspectCreatedTime);

    Map<String, Object> aspectCreatedActor = new HashMap<>();
    aspectCreatedActor.put(TYPE, "keyword");
    aspectCreatedActor.put("copy_to", List.of("_search._system_aspectCreated_actor"));
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
    aspectModifiedTime.put("copy_to", List.of("_search._system_aspectModified_time"));
    aspectModifiedProperties.put("time", aspectModifiedTime);

    Map<String, Object> aspectModifiedActor = new HashMap<>();
    aspectModifiedActor.put(TYPE, "keyword");
    aspectModifiedActor.put("copy_to", List.of("_search._system_aspectModified_actor"));
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
