package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.ESUtils.BOOLEAN_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.COPY_TO;
import static com.linkedin.metadata.search.utils.ESUtils.INDEX;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;
import static com.linkedin.metadata.search.utils.ESUtils.TEXT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.Collection;
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
    return createAspectMappings(
        entitySpec, fieldNameConflicts, fieldNameAliasConflicts, List.of(entitySpec));
  }

  public static Map<String, Object> createAspectMappings(
      @Nonnull EntitySpec entitySpec,
      @Nullable Map<String, Set<String>> fieldNameConflicts,
      @Nullable Map<String, Set<String>> fieldNameAliasConflicts,
      @Nonnull Collection<EntitySpec> indexGroup) {

    Map<String, Object> aspectsMappings = new HashMap<>();

    // Searchable fields are grouped under _aspects.aspectName. structuredProperties stays at the
    // root; its parsed URN names still need an aspect source so a later update can clear them.
    entitySpec.getAspectSpecs().stream()
        .forEach(
            aspectSpec -> {
              String aspectName = aspectSpec.getName();
              boolean rootOnly = STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectName);

              // Regular aspects go under _aspects. A root-only aspect keeps its searchable fields
              // on the document root and stores only the parsed-name sources here.
              Map<String, Object> aspectFields = new HashMap<>();

              if (!rootOnly) {
                aspectSpec
                    .getSearchableFieldSpecs()
                    .forEach(
                        searchableFieldSpec -> {
                          aspectFields.putAll(
                              MultiEntityMappingsBuilder.getAspectMappingsForField(
                                  searchableFieldSpec, aspectName));
                          copySharedHasValues(searchableFieldSpec, aspectFields);
                        });
              }

              addParsedUrnFields(aspectSpec, aspectFields, indexGroup);

              if (!rootOnly) {
                // Add system metadata to each aspect using the projector's serialized field name.
                aspectFields.put(
                    V3SearchDocumentProjector.SYSTEM_METADATA_FIELD, createSystemMetadataMapping());
              }
              if (aspectSpec.getAspectAnnotation() != null
                  && aspectSpec.getAspectAnnotation().isAspectJson()) {
                // A string so doc_as_upsert replaces the aspect JSON. An object would merge, and
                // a cleared field would stay in _source.
                Map<String, Object> stored = new HashMap<>();
                stored.put(TYPE, TEXT_FIELD_TYPE);
                stored.put(INDEX, false);
                aspectFields.put(V3SearchDocumentProjector.ASPECT_JSON_FIELD, stored);
              }

              if (!aspectFields.isEmpty()) {
                aspectsMappings.put(aspectName, ImmutableMap.of(PROPERTIES, aspectFields));
              }
            });

    return aspectsMappings;
  }

  /**
   * One indexed keyword per URN field. {@code copy_to} follows the field's degree-qualified label,
   * or {@code other} when the URN is included in full text and has no label.
   */
  private static void addParsedUrnFields(
      @Nonnull com.linkedin.metadata.models.AspectSpec aspectSpec,
      @Nonnull Map<String, Object> aspectFields,
      @Nonnull Collection<EntitySpec> indexGroup) {
    for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
      if (!V3SearchFields.contributesParsedUrn(fieldSpec)) {
        continue;
      }
      String fieldName = fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
      List<String> destinations = V3SearchFields.parsedNameDestinations(fieldSpec, indexGroup);
      aspectFields.put(
          V3SearchFields.parsedUrnField(fieldName),
          V3SearchFields.parsedUrnKeywordMapping(
              destinations,
              destinations.isEmpty() && V3SearchFields.contributesFullTextUrn(fieldSpec)));
      List<String> fullUrnTargets = V3SearchFields.fullUrnCopyTargets(fieldSpec, indexGroup);
      if (!fullUrnTargets.isEmpty() && aspectFields.get(fieldName) instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> rawField = (Map<String, Object>) aspectFields.get(fieldName);
        rawField.put(COPY_TO, fullUrnTargets);
      }
    }
  }

  /**
   * The aspect boolean keeps the name the document writes and copies onto the degree's root name.
   * Degree 0 copies {@code hasDescription} onto {@code hasDescription}. Degree 1 copies it onto
   * {@code hasDescription_1}. Only a true is written, so the root field never holds both true and
   * false.
   */
  private static void copySharedHasValues(
      @Nonnull SearchableFieldSpec fieldSpec, @Nonnull Map<String, Object> aspectFields) {
    final SearchableAnnotation resolved = fieldSpec.getSearchableAnnotation().resolvedForV3();
    if (!resolved.isV3Searchable() || resolved.indexedFieldName().isEmpty()) {
      return;
    }
    resolved
        .getHasValuesFieldName()
        .ifPresent(
            declaredName -> {
              String rootName = resolved.indexedHasValuesFieldName().orElse(declaredName);
              Map<String, Object> mapping = new HashMap<>();
              mapping.put(TYPE, BOOLEAN_FIELD_TYPE);
              mapping.put(COPY_TO, List.of(rootName));
              aspectFields.put(declaredName, mapping);
            });
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
    lastObserved.put("copy_to", List.of("_system.lastObserved"));
    properties.put("lastObserved", lastObserved);

    // runId field
    Map<String, Object> runId = new HashMap<>();
    runId.put(TYPE, "keyword");
    runId.put("copy_to", List.of("_system.runId"));
    properties.put("runId", runId);

    // lastRunId field
    Map<String, Object> lastRunId = new HashMap<>();
    lastRunId.put(TYPE, "keyword");
    lastRunId.put("copy_to", List.of("_system.lastRunId"));
    properties.put("lastRunId", lastRunId);

    // aspectCreated field
    Map<String, Object> aspectCreated = new HashMap<>();
    aspectCreated.put(TYPE, "object");
    Map<String, Object> aspectCreatedProperties = new HashMap<>();

    Map<String, Object> aspectCreatedTime = new HashMap<>();
    aspectCreatedTime.put(TYPE, "date");
    aspectCreatedTime.put("copy_to", List.of("_system.created.time"));
    aspectCreatedProperties.put("time", aspectCreatedTime);

    Map<String, Object> aspectCreatedActor = new HashMap<>();
    aspectCreatedActor.put(TYPE, "keyword");
    aspectCreatedActor.put("copy_to", List.of("_system.created.actor"));
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
    aspectModifiedTime.put("copy_to", List.of("_system.modified.time"));
    aspectModifiedProperties.put("time", aspectModifiedTime);

    Map<String, Object> aspectModifiedActor = new HashMap<>();
    aspectModifiedActor.put(TYPE, "keyword");
    aspectModifiedActor.put("copy_to", List.of("_system.modified.actor"));
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
