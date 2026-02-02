package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Utility class for detecting and resolving field name conflicts in entity mappings. Handles
 * conflicts between regular field names and field name aliases.
 */
public class ConflictResolver {

  /**
   * Resolves field type conflicts by choosing the most appropriate type. Only resolves [long, date]
   * conflicts by choosing date (more specific for timestamps). All other conflicts are left as-is
   * and will cause mapping errors.
   *
   * @param conflictingTypes set of conflicting Elasticsearch types
   * @return the resolved Elasticsearch type
   */
  @Nonnull
  public static String resolveTypeConflict(@Nonnull Set<String> conflictingTypes) {
    if (conflictingTypes.size() <= 1) {
      return conflictingTypes.iterator().next();
    }

    // Only resolve [long, date] conflicts by choosing date
    if (conflictingTypes.size() == 2
        && conflictingTypes.contains("long")
        && conflictingTypes.contains("date")) {
      return "date"; // DATETIME is more specific than long for timestamps
    }

    // For all other conflicts, throw an exception to maintain existing behavior
    throw new IllegalArgumentException(
        "Field type conflicts detected. Fields with the same name must have the same type. "
            + "Conflicts: "
            + conflictingTypes
            + ". Only [long, date] conflicts are automatically resolved to 'date'.");
  }

  /**
   * Detects conflicts between field names and field name aliases across entity specs.
   *
   * @param entitySpecs collection of entity specs to analyze
   * @return ConflictResult containing field name conflicts and field name alias conflicts
   */
  public static ConflictResult detectConflicts(@Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, Set<String>> fieldNameToPaths = new HashMap<>();
    Map<String, Set<String>> fieldNameAliasToPaths = new HashMap<>();
    Map<String, Set<String>> fieldNameToElasticsearchTypes = new HashMap<>();
    Map<String, Set<String>> fieldNameAliasToElasticsearchTypes = new HashMap<>();

    // Collect all field names and their paths
    for (EntitySpec entitySpec : entitySpecs) {
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        String aspectName = aspectSpec.getName();
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          String fieldName = fieldSpec.getSearchableAnnotation().getFieldName();
          String fieldPath = getFieldPath(aspectName, fieldName);
          SearchableAnnotation.FieldType fieldType =
              fieldSpec.getSearchableAnnotation().getFieldType();

          // Convert PDL field type to Elasticsearch type
          String elasticsearchType = FieldTypeMapper.getElasticsearchTypeForFieldType(fieldType);

          fieldNameToPaths.computeIfAbsent(fieldName, k -> new HashSet<>()).add(fieldPath);
          fieldNameToElasticsearchTypes
              .computeIfAbsent(fieldName, k -> new HashSet<>())
              .add(elasticsearchType);

          // Collect field name aliases
          if (fieldSpec.getSearchableAnnotation().getFieldNameAliases() != null) {
            for (String alias : fieldSpec.getSearchableAnnotation().getFieldNameAliases()) {
              fieldNameAliasToPaths.computeIfAbsent(alias, k -> new HashSet<>()).add(fieldPath);
              fieldNameAliasToElasticsearchTypes
                  .computeIfAbsent(alias, k -> new HashSet<>())
                  .add(elasticsearchType);
            }
          }
        }
      }
    }

    // Detect conflicts
    Map<String, Set<String>> fieldNameConflicts = new HashMap<>();
    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    Map<String, Set<String>> fieldNameTypeConflicts = new HashMap<>();
    Map<String, Set<String>> fieldNameAliasTypeConflicts = new HashMap<>();

    // Find field name conflicts (multiple paths for same field name)
    for (Map.Entry<String, Set<String>> entry : fieldNameToPaths.entrySet()) {
      if (entry.getValue().size() > 1) {
        fieldNameConflicts.put(entry.getKey(), entry.getValue());
        // Check for Elasticsearch type conflicts
        Set<String> elasticsearchTypes = fieldNameToElasticsearchTypes.get(entry.getKey());
        if (elasticsearchTypes.size() > 1) {
          fieldNameTypeConflicts.put(entry.getKey(), elasticsearchTypes);
        }
      }
    }

    // Find field name alias conflicts (multiple paths for same alias)
    for (Map.Entry<String, Set<String>> entry : fieldNameAliasToPaths.entrySet()) {
      if (entry.getValue().size() > 1) {
        fieldNameAliasConflicts.put(entry.getKey(), entry.getValue());
        // Check for Elasticsearch type conflicts
        Set<String> elasticsearchTypes = fieldNameAliasToElasticsearchTypes.get(entry.getKey());
        if (elasticsearchTypes.size() > 1) {
          fieldNameAliasTypeConflicts.put(entry.getKey(), elasticsearchTypes);
        }
      }
    }

    // Check for conflicts between field names and field name aliases
    for (String fieldName : fieldNameToPaths.keySet()) {
      if (fieldNameAliasToPaths.containsKey(fieldName)) {
        // Field name conflicts with a field name alias
        Set<String> allPaths = new HashSet<>();
        allPaths.addAll(fieldNameToPaths.get(fieldName));
        allPaths.addAll(fieldNameAliasToPaths.get(fieldName));

        Set<String> allElasticsearchTypes = new HashSet<>();
        allElasticsearchTypes.addAll(fieldNameToElasticsearchTypes.get(fieldName));
        allElasticsearchTypes.addAll(fieldNameAliasToElasticsearchTypes.get(fieldName));

        fieldNameConflicts.put(fieldName, allPaths);
        fieldNameAliasConflicts.put(fieldName, allPaths);
        if (allElasticsearchTypes.size() > 1) {
          fieldNameTypeConflicts.put(fieldName, allElasticsearchTypes);
          fieldNameAliasTypeConflicts.put(fieldName, allElasticsearchTypes);
        }
      }
    }

    return new ConflictResult(
        fieldNameConflicts,
        fieldNameAliasConflicts,
        fieldNameTypeConflicts,
        fieldNameAliasTypeConflicts);
  }

  /**
   * Collects field name aliases and their paths from an entity spec.
   *
   * @param entitySpec the entity spec to analyze
   * @return map of field name aliases to their paths
   */
  public static Map<String, Set<String>> collectFieldNameAliasPaths(
      @Nonnull EntitySpec entitySpec) {
    Map<String, Set<String>> fieldNameAliasToPaths = new HashMap<>();

    for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
      String aspectName = aspectSpec.getName();
      for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
        if (fieldSpec.getSearchableAnnotation().getFieldNameAliases() != null) {
          for (String alias : fieldSpec.getSearchableAnnotation().getFieldNameAliases()) {
            String fieldPath =
                getFieldPath(aspectName, fieldSpec.getSearchableAnnotation().getFieldName());
            fieldNameAliasToPaths.computeIfAbsent(alias, k -> new HashSet<>()).add(fieldPath);
          }
        }
      }
    }

    return fieldNameAliasToPaths;
  }

  /**
   * Generates a field path from aspect name and field name.
   *
   * @param aspectName the aspect name
   * @param fieldName the field name
   * @return the field path
   */
  private static String getFieldPath(@Nonnull String aspectName, @Nonnull String fieldName) {
    return "_aspects." + aspectName + MappingConstants.ASPECT_FIELD_DELIMITER + fieldName;
  }

  /** Result class containing conflict information. */
  public static class ConflictResult {
    private final Map<String, Set<String>> fieldNameConflicts;
    private final Map<String, Set<String>> fieldNameAliasConflicts;
    private final Map<String, Set<String>> fieldNameTypeConflicts;
    private final Map<String, Set<String>> fieldNameAliasTypeConflicts;

    public ConflictResult(
        @Nonnull Map<String, Set<String>> fieldNameConflicts,
        @Nonnull Map<String, Set<String>> fieldNameAliasConflicts,
        @Nonnull Map<String, Set<String>> fieldNameTypeConflicts,
        @Nonnull Map<String, Set<String>> fieldNameAliasTypeConflicts) {
      this.fieldNameConflicts = fieldNameConflicts;
      this.fieldNameAliasConflicts = fieldNameAliasConflicts;
      this.fieldNameTypeConflicts = fieldNameTypeConflicts;
      this.fieldNameAliasTypeConflicts = fieldNameAliasTypeConflicts;
    }

    @Nonnull
    public Map<String, Set<String>> getFieldNameConflicts() {
      return fieldNameConflicts;
    }

    @Nonnull
    public Map<String, Set<String>> getFieldNameAliasConflicts() {
      return fieldNameAliasConflicts;
    }

    @Nonnull
    public Map<String, Set<String>> getFieldNameTypeConflicts() {
      return fieldNameTypeConflicts;
    }

    @Nonnull
    public Map<String, Set<String>> getFieldNameAliasTypeConflicts() {
      return fieldNameAliasTypeConflicts;
    }

    public boolean hasConflicts() {
      return !fieldNameConflicts.isEmpty() || !fieldNameAliasConflicts.isEmpty();
    }

    public boolean hasTypeConflicts() {
      return !fieldNameTypeConflicts.isEmpty() || !fieldNameAliasTypeConflicts.isEmpty();
    }

    /**
     * Checks if a specific field has type conflicts (multiple different Elasticsearch types).
     *
     * @param fieldName the field name to check
     * @return true if the field has type conflicts, false otherwise
     */
    public boolean hasTypeConflictForField(@Nonnull String fieldName) {
      Set<String> types = fieldNameTypeConflicts.get(fieldName);
      return types != null && types.size() > 1;
    }

    /**
     * Checks if a specific field alias has type conflicts (multiple different Elasticsearch types).
     *
     * @param alias the field alias to check
     * @return true if the alias has type conflicts, false otherwise
     */
    public boolean hasTypeConflictForAlias(@Nonnull String alias) {
      Set<String> types = fieldNameAliasTypeConflicts.get(alias);
      return types != null && types.size() > 1;
    }
  }
}
