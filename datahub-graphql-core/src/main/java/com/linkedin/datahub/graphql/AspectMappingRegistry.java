package com.linkedin.datahub.graphql;

import graphql.language.ArrayValue;
import graphql.language.StringValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AspectMappingRegistry {
  private final Map<String, Set<String>> fieldToAspects = new HashMap<>();

  public AspectMappingRegistry(GraphQLSchema schema) {
    buildMappingFromSchema(schema);
  }

  private void buildMappingFromSchema(GraphQLSchema schema) {
    schema
        .getTypeMap()
        .values()
        .forEach(
            type -> {
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
                                      .collect(Collectors.toSet());

                              String key = typeName + "." + fieldName;
                              fieldToAspects.put(key, aspects);
                              log.debug(
                                  "Mapped {}.{} to aspects: {}", typeName, fieldName, aspects);
                            }
                          } else if (noAspectsDirective != null) {
                            String key = typeName + "." + fieldName;
                            fieldToAspects.put(key, new HashSet<>());
                            log.debug(
                                "Mapped {}.{} to to request no specific aspects.",
                                typeName,
                                fieldName);
                          }
                        });
              }
            });

    log.info("Built aspect mapping registry with {} field mappings", fieldToAspects.size());
  }

  /**
   * Get required aspects for the given fields on a type. Returns null if any field is unmapped
   * (fallback to all aspects).
   */
  @Nullable
  public Set<String> getRequiredAspects(
      String typeName, List<graphql.schema.SelectedField> requestedFields) {
    Set<String> aspects = new HashSet<>();

    for (graphql.schema.SelectedField field : requestedFields) {
      // Skip introspection and nested fields (with level > 2 since top level fields are level 2 ie.
      // Dataset.urn)
      String fieldName = field.getName();
      if (fieldName.startsWith("__") || field.getLevel() > 2) {
        continue;
      }

      String key = typeName + "." + fieldName;
      Set<String> fieldAspects = fieldToAspects.get(key);

      if (fieldAspects != null) {
        aspects.addAll(fieldAspects);
      } else {
        // Unmapped field - fallback to all aspects
        log.debug(
            "Field {} has no @aspectMapping or @noAspects directives, will fetch all aspects", key);
        return null;
      }
    }

    return aspects.isEmpty() ? Collections.emptySet() : aspects;
  }
}
