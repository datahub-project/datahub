package com.linkedin.datahub.graphql;

import graphql.language.ArrayValue;
import graphql.language.StringValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps GraphQL field selections to the minimum set of aspects needed to resolve them, enabling
 * performance optimization by fetching only required aspects instead of all aspects.
 *
 * <p>This class scans the GraphQL schema for two directives: - @aspectMapping(aspects:
 * ["aspectName"]) - declares which aspects a field needs - @noAspects - indicates a field needs no
 * aspects (computed fields, custom resolvers)
 *
 * <p>To use in entity types, add one line to batchLoad: Set&lt;String&gt; aspects =
 * AspectUtils.getOptimizedAspects(context, name(), ALL_ASPECTS, DATASET_KEY_ASPECT_NAME);
 *
 * <p>If any field lacks a mapping directive, getRequiredAspects returns null and the entity type
 * falls back to fetching all aspects for safety.
 */
@Slf4j
public class AspectMappingRegistry {
  private final Map<String, Set<String>> fieldToAspects;

  /**
   * Fields marked {@code @fetchAllAspects}: their value depends on all of an entity's aspects (e.g.
   * lastIngested is computed from the system metadata of every fetched aspect), so selecting them
   * forces the fetch-all fallback deliberately rather than via the unmapped-field warning.
   */
  private final Set<String> fetchAllFields;

  private final Set<String> warnedUnmappedFields = ConcurrentHashMap.newKeySet();

  public AspectMappingRegistry(GraphQLSchema schema) {
    Set<String> fetchAll = ConcurrentHashMap.newKeySet();
    this.fieldToAspects = Collections.unmodifiableMap(buildMappingFromSchema(schema, fetchAll));
    this.fetchAllFields = Collections.unmodifiableSet(fetchAll);
  }

  private Map<String, Set<String>> buildMappingFromSchema(
      GraphQLSchema schema, Set<String> fetchAllFieldsSink) {
    Map<String, Set<String>> mapping = new HashMap<>();
    schema
        .getTypeMap()
        .values()
        .forEach(
            type -> {
              // Only object types carry field definitions with @aspectMapping / @noAspects.
              // Interfaces/unions are skipped: concrete implementing types own the annotations,
              // and SelectedField.getObjectTypeNames() already attributes fields to those types.
              if (type instanceof GraphQLObjectType) {
                GraphQLObjectType objectType = (GraphQLObjectType) type;
                String typeName = objectType.getName();

                objectType
                    .getFieldDefinitions()
                    .forEach(
                        field -> {
                          String fieldName = field.getName();
                          GraphQLDirective aspectsDirective = field.getDirective("aspectMapping");
                          GraphQLDirective noAspectsDirective = field.getDirective("noAspects");

                          if (aspectsDirective != null) {
                            GraphQLArgument aspectsArg = aspectsDirective.getArgument("aspects");
                            if (aspectsArg != null
                                && aspectsArg.getArgumentValue().getValue() instanceof ArrayValue) {
                              ArrayValue aspectsArray =
                                  (ArrayValue) aspectsArg.getArgumentValue().getValue();
                              Set<String> aspects =
                                  aspectsArray.getValues().stream()
                                      .map(value -> ((StringValue) value).getValue())
                                      .collect(Collectors.toUnmodifiableSet());

                              String key = typeName + "." + fieldName;
                              mapping.put(key, aspects);
                              log.debug(
                                  "Mapped {}.{} to aspects: {}", typeName, fieldName, aspects);
                            }
                          } else if (noAspectsDirective != null) {
                            String key = typeName + "." + fieldName;
                            mapping.put(key, Set.of());
                            log.debug(
                                "Mapped {}.{} to request no specific aspects.",
                                typeName,
                                fieldName);
                          } else if (field.getDirective("fetchAllAspects") != null) {
                            String key = typeName + "." + fieldName;
                            fetchAllFieldsSink.add(key);
                            log.debug("Mapped {}.{} to force fetch-all.", typeName, fieldName);
                          }
                        });
              }
            });

    log.info("Built aspect mapping registry with {} field mappings", mapping.size());
    return mapping;
  }

  /**
   * Get required aspects for the given fields on a type. Returns null if any field is unmapped
   * (fallback to all aspects).
   *
   * <p>This method filters the selection set to only include fields that directly belong to the
   * specified type, regardless of where that type appears in the query tree. This allows it to work
   * correctly for both top-level queries and nested entities (e.g., Dataset inside SearchResult).
   */
  @Nullable
  public Set<String> getRequiredAspects(
      String typeName, Collection<graphql.schema.SelectedField> requestedFields) {
    Set<String> fieldNames = new HashSet<>();
    for (graphql.schema.SelectedField field : requestedFields) {
      // Only process fields that belong to the target type
      // getObjectTypeNames() returns the set of types this field belongs to (accounting for
      // interfaces/unions)
      if (field.getObjectTypeNames().contains(typeName)) {
        fieldNames.add(field.getName());
      }
    }
    return getRequiredAspectsForFieldNames(typeName, fieldNames);
  }

  /**
   * Get required aspects for the named fields selected directly on {@code typeName}. Returns null
   * if any field is unmapped (fallback to all aspects).
   *
   * <p>Callers are responsible for restricting {@code fieldNames} to fields that actually belong to
   * {@code typeName} — see {@link com.linkedin.datahub.graphql.util.SelectionSetAnalyzer}.
   */
  @Nullable
  public Set<String> getRequiredAspectsForFieldNames(
      String typeName, Collection<String> fieldNames) {
    Set<String> aspects = new HashSet<>();

    for (String fieldName : fieldNames) {
      // Skip introspection fields
      if (fieldName.startsWith("__")) {
        continue;
      }

      String key = typeName + "." + fieldName;

      if (fetchAllFields.contains(key)) {
        // Deliberate fetch-all (e.g. lastIngested depends on every aspect's system metadata).
        log.debug("Field {} is @fetchAllAspects, fetching all aspects", key);
        return null;
      }

      Set<String> fieldAspects = fieldToAspects.get(key);

      if (fieldAspects != null) {
        aspects.addAll(fieldAspects);
        log.debug("Field {} mapped to aspects: {}", key, fieldAspects);
      } else {
        // Unmapped field - fallback to all aspects to be conservative
        if (warnedUnmappedFields.add(key)) {
          log.warn(
              "Field {} has no @aspectMapping or @noAspects directives, will fetch all aspects",
              key);
        } else {
          log.debug(
              "Field {} has no @aspectMapping or @noAspects directives, will fetch all aspects",
              key);
        }
        return null;
      }
    }

    log.debug("Computed required aspects for {}: {}", typeName, aspects);
    return aspects.isEmpty() ? Collections.emptySet() : aspects;
  }
}
