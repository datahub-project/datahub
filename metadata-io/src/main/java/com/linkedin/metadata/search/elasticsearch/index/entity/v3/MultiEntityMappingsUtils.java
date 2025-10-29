package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.PATH;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.search.elasticsearch.index.BaseConfigurationLoader;
import com.linkedin.metadata.search.utils.ESUtils;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for common mapping operations used across different mapping builders. This
 * consolidates shared functionality to avoid code duplication.
 */
@Slf4j
public class MultiEntityMappingsUtils {

  private MultiEntityMappingsUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates an alias mapping for _entityName that points to _search.entityName. This consolidates
   * the _entityName handling logic used in multiple places.
   *
   * @return Map containing the alias mapping for _entityName
   */
  public static Map<String, Object> createEntityNameAliasMapping() {
    Map<String, Object> aliasMapping = new HashMap<>();
    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
    aliasMapping.put(PATH, "_search.entityName");
    return aliasMapping;
  }

  /**
   * Checks if a field name or alias is _entityName.
   *
   * @param fieldName the field name or alias to check
   * @return true if the field name is _entityName
   */
  public static boolean isEntityNameField(String fieldName) {
    return "_entityName".equals(fieldName);
  }

  /**
   * Creates an alias mapping for _qualifiedName that points to _search.qualifiedName. This
   * consolidates the _qualifiedName handling logic used in multiple places.
   *
   * @return Map containing the alias mapping for _qualifiedName
   */
  public static Map<String, Object> createQualifiedNameAliasMapping() {
    Map<String, Object> aliasMapping = new HashMap<>();
    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
    aliasMapping.put(PATH, "_search.qualifiedName");
    return aliasMapping;
  }

  /**
   * Checks if a field name or alias is _qualifiedName.
   *
   * @param fieldName the field name or alias to check
   * @return true if the field name is _qualifiedName
   */
  public static boolean isQualifiedNameField(String fieldName) {
    return "_qualifiedName".equals(fieldName);
  }

  /**
   * Creates a generic alias mapping that points to a specified path.
   *
   * @param targetPath the path the alias should point to
   * @return Map containing the alias mapping
   */
  public static Map<String, Object> createAliasMapping(String targetPath) {
    Map<String, Object> aliasMapping = new HashMap<>();
    aliasMapping.put(TYPE, ALIAS_FIELD_TYPE);
    aliasMapping.put(PATH, targetPath);
    return aliasMapping;
  }

  /**
   * Checks if a field has other copy_to destinations besides tier fields.
   *
   * @param searchableFieldSpec the field specification to check
   * @return true if the field has other copy_to destinations
   */
  public static boolean hasOtherCopyToDestinations(SearchableFieldSpec searchableFieldSpec) {
    com.linkedin.metadata.models.annotation.SearchableAnnotation annotation =
        searchableFieldSpec.getSearchableAnnotation();

    // Check for searchLabel
    if (annotation.getSearchLabel().isPresent()
        && annotation.getSearchLabel().get() != null
        && !annotation.getSearchLabel().get().isEmpty()) {
      return true;
    }

    // Check for entityFieldName
    if (annotation.getEntityFieldName().isPresent()
        && annotation.getEntityFieldName().get() != null
        && !annotation.getEntityFieldName().get().isEmpty()) {
      return true;
    }

    return false;
  }

  /**
   * Checks if a field has aliases that would require it to remain indexed.
   *
   * @param searchableFieldSpec the field specification to check
   * @return true if the field has aliases
   */
  public static boolean hasFieldAliases(SearchableFieldSpec searchableFieldSpec) {
    com.linkedin.metadata.models.annotation.SearchableAnnotation annotation =
        searchableFieldSpec.getSearchableAnnotation();

    // Check for fieldNameAliases
    if (annotation.getFieldNameAliases() != null && !annotation.getFieldNameAliases().isEmpty()) {
      return true;
    }

    return false;
  }

  /**
   * Finds the field type for a given field name and aspect name across entity specs.
   *
   * @param entitySpecs all entity specs
   * @param fieldName the field name to find
   * @param aspectName the aspect name to search in
   * @return the field type, or KEYWORD as default
   */
  public static FieldType findFieldTypeForFieldName(
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull String fieldName,
      @Nonnull String aspectName) {

    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (aspectName.equals(aspectSpec.getName())) {
          for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
            if (fieldName.equals(fieldSpec.getSearchableAnnotation().getFieldName())) {
              return fieldSpec.getSearchableAnnotation().getFieldType();
            }
          }
        }
      }
    }

    log.warn(
        "Could not find field type for field '{}' in aspect '{}', defaulting to KEYWORD",
        fieldName,
        aspectName);
    return FieldType.KEYWORD;
  }

  /**
   * Checks if any of the fields with the given field name across all paths have eagerGlobalOrdinals
   * set to true.
   *
   * @param entitySpecs collection of entity specs to search
   * @param fieldName the field name to check
   * @param allPaths set of all paths for the field name
   * @return true if any field has eagerGlobalOrdinals set to true
   */
  public static boolean hasEagerGlobalOrdinals(
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull String fieldName,
      @Nonnull java.util.Set<String> allPaths) {

    for (String path : allPaths) {
      String[] pathParts = path.split("\\.");
      if (pathParts.length >= 2) {
        String aspectName = pathParts[1];

        // Find the entity spec that contains this aspect
        for (EntitySpec entitySpec : entitySpecs) {
          AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
          if (aspectSpec != null) {
            // Check if any searchable field in this aspect has the field name and
            // eagerGlobalOrdinals set
            for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
              if (fieldName.equals(searchableFieldSpec.getSearchableAnnotation().getFieldName())) {
                if (searchableFieldSpec
                    .getSearchableAnnotation()
                    .getEagerGlobalOrdinals()
                    .orElse(false)) {
                  return true;
                }
              }
            }
          }
        }
      }
    }

    return false;
  }

  /**
   * Loads mapping configuration from a resource file.
   *
   * @param resourcePath the path to the resource file
   * @return Map containing the mapping configuration
   * @throws IOException if the resource cannot be read
   */
  public static Map<String, Object> loadMappingConfigurationFromResource(String resourcePath)
      throws IOException {
    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource(resourcePath);
    return BaseConfigurationLoader.extractMappingsSection(config, resourcePath);
  }

  /**
   * Merges mapping configuration with existing mappings. This allows the configuration to override
   * or extend existing mappings.
   *
   * @param existingMappings existing mappings to merge with
   * @param configMappings configuration mappings to merge
   * @return merged mappings
   */
  public static Map<String, Object> mergeMappings(
      Map<String, Object> existingMappings, Map<String, Object> configMappings) {
    if (existingMappings == null) {
      return configMappings != null ? new HashMap<>(configMappings) : new HashMap<>();
    }

    if (configMappings == null) {
      return new HashMap<>(existingMappings);
    }

    Map<String, Object> mergedMappings = new HashMap<>(existingMappings);

    // Merge dynamic templates
    if (configMappings.containsKey("dynamic_templates")) {
      Object dynamicTemplates = configMappings.get("dynamic_templates");
      // The YAML structure is already correct for OpenSearch, so we can use it directly
      mergedMappings.put("dynamic_templates", dynamicTemplates);
    }

    // Merge dynamic setting
    if (configMappings.containsKey("dynamic")) {
      mergedMappings.put("dynamic", configMappings.get("dynamic"));
    }

    // Merge properties
    if (configMappings.containsKey("properties")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> existingProperties =
          (Map<String, Object>) existingMappings.get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> configProperties = (Map<String, Object>) configMappings.get("properties");

      if (existingProperties != null && configProperties != null) {
        Map<String, Object> mergedProperties = new HashMap<>(existingProperties);

        // Special handling for _aspects - merge the individual aspects instead of overwriting
        if (configProperties.containsKey("_aspects")
            && existingProperties.containsKey("_aspects")) {
          @SuppressWarnings("unchecked")
          Map<String, Object> existingAspects =
              (Map<String, Object>) existingProperties.get("_aspects");
          @SuppressWarnings("unchecked")
          Map<String, Object> configAspects =
              (Map<String, Object>) configProperties.get("_aspects");

          if (existingAspects.containsKey("properties")
              && configAspects.containsKey("properties")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> existingAspectsProperties =
                (Map<String, Object>) existingAspects.get("properties");
            @SuppressWarnings("unchecked")
            Map<String, Object> configAspectsProperties =
                (Map<String, Object>) configAspects.get("properties");

            Map<String, Object> mergedAspectsProperties = new HashMap<>(existingAspectsProperties);
            mergedAspectsProperties.putAll(configAspectsProperties);

            Map<String, Object> mergedAspects = new HashMap<>(existingAspects);
            mergedAspects.put("properties", mergedAspectsProperties);
            mergedProperties.put("_aspects", mergedAspects);
          }
        }

        // Merge all other properties normally
        for (Map.Entry<String, Object> entry : configProperties.entrySet()) {
          if (!"_aspects".equals(entry.getKey())) {
            mergedProperties.put(entry.getKey(), entry.getValue());
          }
        }

        mergedMappings.put("properties", mergedProperties);
      } else if (configProperties != null) {
        mergedMappings.put("properties", configProperties);
      }
    }

    return mergedMappings;
  }

  /**
   * Gets the Elasticsearch type for a field based on its SearchableAnnotation.
   *
   * @param fieldSpec the field specification
   * @return the Elasticsearch type
   */
  public static String getElasticsearchTypeForField(SearchableFieldSpec fieldSpec) {
    com.linkedin.metadata.models.annotation.SearchableAnnotation annotation =
        fieldSpec.getSearchableAnnotation();
    FieldType fieldType = annotation.getFieldType();

    // For searchIndexed fields, use keyword type
    if (annotation.getSearchIndexed().orElse(false)) {
      return ESUtils.KEYWORD_FIELD_TYPE;
    }

    return FieldTypeMapper.getElasticsearchTypeForFieldType(fieldType);
  }

  /**
   * Builds the _search section with all copy_to destination fields. Collects all fields that are
   * copied to _search.* destinations and ensures they are properly defined with appropriate types.
   *
   * @param entitySpecs collection of entity specs to analyze
   * @param mappings existing mappings to analyze for copy_to destinations
   * @return Map containing the _search section properties
   */
  public static Map<String, Object> buildSearchSection(
      Collection<EntitySpec> entitySpecs, Map<String, Object> mappings) {
    Map<String, Object> searchProperties = new HashMap<>();
    Map<String, Set<String>> searchFieldTypes = new HashMap<>();

    // Collect all copy_to destinations that point to _search.* fields
    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          com.linkedin.metadata.models.annotation.SearchableAnnotation annotation =
              fieldSpec.getSearchableAnnotation();

          // Check for search label destinations
          annotation
              .getSearchLabel()
              .ifPresent(
                  searchLabel -> {
                    if (searchLabel != null && !searchLabel.isEmpty()) {
                      String destinationField = searchLabel;
                      String fieldType = getElasticsearchTypeForField(fieldSpec);
                      searchFieldTypes
                          .computeIfAbsent(destinationField, k -> new HashSet<>())
                          .add(fieldType);
                    }
                  });

          // Check for entity field name destinations
          annotation
              .getEntityFieldName()
              .ifPresent(
                  entityFieldName -> {
                    if (entityFieldName != null && !entityFieldName.isEmpty()) {
                      String destinationField = entityFieldName;
                      String fieldType = getElasticsearchTypeForField(fieldSpec);
                      searchFieldTypes
                          .computeIfAbsent(destinationField, k -> new HashSet<>())
                          .add(fieldType);
                    }
                  });
        }
      }
    }

    // Create field mappings for each destination field, resolving type conflicts
    for (Map.Entry<String, Set<String>> entry : searchFieldTypes.entrySet()) {
      String fieldName = entry.getKey();
      Set<String> types = entry.getValue();

      String resolvedType = ConflictResolver.resolveTypeConflict(types);

      Map<String, Object> fieldMapping = new HashMap<>();
      fieldMapping.put("type", resolvedType);

      // Add normalizer for keyword fields
      if ("keyword".equals(resolvedType)) {
        fieldMapping.put("normalizer", "keyword_normalizer");
      }

      searchProperties.put(fieldName, fieldMapping);
    }

    // Wrap search properties in a properties object to match Elasticsearch structure
    Map<String, Object> searchSection = new HashMap<>();
    searchSection.put("dynamic", true);
    searchSection.put("properties", searchProperties);

    return searchSection;
  }
}
