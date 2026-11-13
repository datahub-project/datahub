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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

              if (!rootOnly && hasSearchables(aspectSpec)) {
                // System metadata copies into the root _system fields. An aspect with nothing
                // searchable does not join the document, so it does not get this tree.
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

  private static boolean hasSearchables(
      @Nonnull com.linkedin.metadata.models.AspectSpec aspectSpec) {
    return hasSpecs(aspectSpec.getSearchableFieldSpecs())
        || hasSpecs(aspectSpec.getSearchableRefFieldSpecs());
  }

  private static boolean hasSpecs(@Nullable List<?> specs) {
    return specs != null && !specs.isEmpty();
  }

  /**
   * Parsed-name keyword for a URN that {@code copy_to}s a search label or {@code other}. A
   * dedicated root does not get this field.
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
      List<String> fullUrnTargets = V3SearchFields.fullUrnCopyTargets(fieldSpec, indexGroup);
      if (!fullUrnTargets.isEmpty() && aspectFields.get(fieldName) instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> rawField = (Map<String, Object>) aspectFields.get(fieldName);
        rawField.put(COPY_TO, fullUrnTargets);
      }
      if (!V3SearchFields.keepsAspectParsedName(fieldSpec, indexGroup)) {
        continue;
      }
      List<String> destinations =
          V3SearchFields.parsedNameDestinations(fieldSpec, indexGroup).stream()
              .filter(
                  destination -> !V3SearchFields.dedicatedParsedNameRoot(destination, indexGroup))
              .toList();
      aspectFields.put(
          V3SearchFields.parsedUrnField(fieldName),
          V3SearchFields.parsedUrnKeywordMapping(
              destinations,
              destinations.isEmpty() && V3SearchFields.contributesFullTextUrn(fieldSpec)));
    }
  }

  /**
   * The aspect boolean keeps the name the document writes and copies onto the degree's root name.
   * Degree 0 copies {@code hasDescription} onto {@code hasDescription}. Degree 1 copies it onto
   * {@code hasDescription_1}. A {@code v3.hasValuesFieldName} is an additional copy target, so
   * {@code hasParentNode} also copies onto {@code hasParent}. Only a true is written, so the root
   * field never holds both true and false.
   */
  private static void copySharedHasValues(
      @Nonnull SearchableFieldSpec fieldSpec, @Nonnull Map<String, Object> aspectFields) {
    final SearchableAnnotation resolved = fieldSpec.getSearchableAnnotation().resolvedForV3();
    if (!resolved.isV3Searchable() || resolved.indexedFieldName().isEmpty()) {
      return;
    }
    Optional<String> declared = resolved.getHasValuesFieldName();
    if (declared != null && declared.isPresent()) {
      String rootName = resolved.indexedHasValuesFieldName().orElse(declared.get());
      List<String> destinations = new ArrayList<>();
      destinations.add(rootName);
      SearchableAnnotation.sharedHasValuesName(resolved)
          .filter(name -> !destinations.contains(name))
          .ifPresent(destinations::add);
      putHasValuesCopy(aspectFields, declared.get(), destinations);
      return;
    }
    SearchableAnnotation.sharedHasValuesName(resolved)
        .ifPresent(name -> putHasValuesCopy(aspectFields, name, List.of(name)));
  }

  private static void putHasValuesCopy(
      @Nonnull Map<String, Object> aspectFields,
      @Nonnull String fieldName,
      @Nonnull List<String> destinations) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, BOOLEAN_FIELD_TYPE);
    mapping.put(COPY_TO, destinations);
    aspectFields.put(fieldName, mapping);
  }

  /**
   * Creates the mapping structure for the _systemMetadata field that appears in each aspect. This
   * replaces the need for a dynamic template by explicitly defining the structure.
   *
   * <p>Only created and modified actor and time are mapped. They copy into the root {@code
   * _system.created} and {@code _system.modified} fields and stay indexed here. Run ids, last
   * observed, and impersonator stay in the aspect document and are not mapped.
   *
   * @return mapping configuration for _systemMetadata field
   */
  private static Map<String, Object> createSystemMetadataMapping() {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, "object");

    Map<String, Object> properties = new HashMap<>();
    properties.put("aspectCreated", auditStampMapping("_system.created"));
    properties.put("aspectModified", auditStampMapping("_system.modified"));

    mapping.put(PROPERTIES, properties);
    return mapping;
  }

  /** Actor and time for one audit stamp, copied into the matching root {@code _system} object. */
  private static Map<String, Object> auditStampMapping(@Nonnull String rootPrefix) {
    Map<String, Object> stamp = new HashMap<>();
    stamp.put(TYPE, "object");
    Map<String, Object> properties = new HashMap<>();
    properties.put("time", indexedSystemField("date", rootPrefix + ".time"));
    properties.put("actor", indexedSystemField("keyword", rootPrefix + ".actor"));
    stamp.put(PROPERTIES, properties);
    return stamp;
  }

  /**
   * A system-metadata field that stays indexed under {@code _aspects}. Omitting {@code index} and
   * {@code doc_values} leaves both on. {@code copy_to} fills a root {@code _system} field when one
   * exists.
   */
  private static Map<String, Object> indexedSystemField(
      @Nonnull String type, @Nullable String rootField) {
    Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, type);
    if (rootField != null) {
      mapping.put("copy_to", List.of(rootField));
    }
    return mapping;
  }
}
