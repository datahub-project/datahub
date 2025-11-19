package com.linkedin.metadata.search.semantic;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Extracts searchable text fields from entity specifications for use in semantic search embedding
 * generation. This class identifies fields annotated with @Searchable(fieldType: TEXT) and builds a
 * mapping of entity types to their searchable text fields.
 */
@Slf4j
public class SearchableFieldExtractor {

  private final EntityRegistry entityRegistry;
  private final Map<String, List<SearchableTextField>> entityToFieldsCache;

  /**
   * Relevant field types for semantic search embeddings. These field types contain textual content
   * that should be included in embedding generation.
   */
  private static final Set<SearchableAnnotation.FieldType> EMBEDDING_FIELD_TYPES =
      Set.of(
          SearchableAnnotation.FieldType.TEXT,
          SearchableAnnotation.FieldType.TEXT_PARTIAL,
          SearchableAnnotation.FieldType.WORD_GRAM);

  public SearchableFieldExtractor(@Nonnull EntityRegistry entityRegistry) {
    this.entityRegistry = entityRegistry;
    this.entityToFieldsCache = new HashMap<>();
    buildCache();
  }

  /**
   * Pre-compute the mapping of entity types to searchable text fields by iterating through all
   * entities in the registry.
   */
  private void buildCache() {
    log.info("Building searchable text field cache for semantic search");

    for (String entityName : entityRegistry.getEntitySpecs().keySet()) {
      try {
        List<SearchableTextField> fields = extractFieldsForEntity(entityName);
        entityToFieldsCache.put(entityName.toLowerCase(), fields);
        log.debug(
            "Extracted {} searchable text fields for entity type: {}", fields.size(), entityName);
      } catch (Exception e) {
        log.error("Failed to extract searchable fields for entity: {}", entityName, e);
      }
    }

    log.info("Searchable text field cache built for {} entity types", entityToFieldsCache.size());
  }

  /**
   * Extract all searchable text fields for a given entity type.
   *
   * @param entityName The name of the entity type (e.g., "dataset", "chart")
   * @return List of SearchableTextField objects representing text fields to include in embeddings
   */
  private List<SearchableTextField> extractFieldsForEntity(@Nonnull String entityName) {
    List<SearchableTextField> result = new ArrayList<>();

    EntitySpec entitySpec = entityRegistry.getEntitySpecs().get(entityName);
    if (entitySpec == null) {
      log.warn("No entity spec found for entity: {}", entityName);
      return result;
    }

    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      String aspectName = aspectSpec.getName();
      for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
        SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
        if (EMBEDDING_FIELD_TYPES.contains(annotation.getFieldType())) {
          String pathSpec = fieldSpec.getPath().toString();
          // PathSpec.toString() adds a leading slash, strip it for consistency
          if (pathSpec.startsWith("/")) {
            pathSpec = pathSpec.substring(1);
          }
          boolean isNested = pathSpec.contains("*");

          SearchableTextField textField =
              SearchableTextField.builder()
                  .fieldPath(pathSpec)
                  .aspectName(aspectName)
                  .nested(isNested)
                  .searchableFieldName(annotation.getFieldName())
                  .build();
          result.add(textField);
          log.debug(
              "Found text field for {}.{}: {} (nested={})",
              entityName,
              aspectName,
              pathSpec,
              isNested);
        }
      }
    }

    return result;
  }

  /**
   * Get the list of searchable text fields for a given entity type.
   *
   * @param entityName The name of the entity type (e.g., "dataset", "chart")
   * @return List of SearchableTextField objects, or empty list if entity not found
   */
  @Nonnull
  public List<SearchableTextField> getSearchableTextFields(@Nonnull String entityName) {
    String normalizedName = entityName.toLowerCase();
    return entityToFieldsCache.getOrDefault(normalizedName, List.of());
  }

  /**
   * Check if an entity type has any searchable text fields.
   *
   * @param entityName The name of the entity type
   * @return true if the entity has searchable text fields, false otherwise
   */
  public boolean hasSearchableTextFields(@Nonnull String entityName) {
    return !getSearchableTextFields(entityName).isEmpty();
  }

  /**
   * Get all entity types that have searchable text fields.
   *
   * @return Set of entity names that have text fields suitable for embeddings
   */
  @Nonnull
  public Set<String> getEntitiesWithTextFields() {
    return entityToFieldsCache.keySet();
  }
}
