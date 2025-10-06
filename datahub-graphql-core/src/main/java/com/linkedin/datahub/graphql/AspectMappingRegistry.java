package com.linkedin.datahub.graphql;

//import graphql.schema.GraphQLArgument;
//import graphql.schema.GraphQLDirective;
//import graphql.schema.GraphQLObjectType;
//import graphql.schema.GraphQLSchema;
//import graphql.language.ArrayValue;
//import graphql.language.StringValue;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class AspectMappingRegistry {
    private final Map<String, Set<String>> fieldToAspects = new HashMap<>();

    public AspectMappingRegistry() {
//        buildMappingFromSchema(schema);
    }

//    private void buildMappingFromSchema(GraphQLSchema schema) {
//        schema.getTypeMap().values().forEach(type -> {
//            if (type instanceof GraphQLObjectType) {
//                GraphQLObjectType objectType = (GraphQLObjectType) type;
//                String typeName = objectType.getName();
//
//                objectType.getFieldDefinitions().forEach(field -> {
//                    String fieldName = field.getName();
//                    GraphQLDirective directive = field.getDirective("aspectMapping");
//
//                    if (directive != null) {
//                        GraphQLArgument aspectsArg = directive.getArgument("aspects");
//                        if (aspectsArg != null && aspectsArg.getArgumentValue().getValue() instanceof ArrayValue) {
//                            ArrayValue aspectsArray = (ArrayValue) aspectsArg.getArgumentValue().getValue();
//                            Set<String> aspects = aspectsArray.getValues().stream()
//                                    .map(value -> ((StringValue) value).getValue())
//                                    .collect(Collectors.toSet());
//
//                            String key = typeName + "." + fieldName;
//                            fieldToAspects.put(key, aspects);
//                            log.debug("Mapped {}.{} to aspects: {}", typeName, fieldName, aspects);
//                        }
//                    }
//                });
//            }
//        });
//
//        log.info("Built aspect mapping registry with {} field mappings", fieldToAspects.size());
//    }

    /**
     * Get required aspects for the given fields on a type.
     * Returns null if any field is unmapped (fallback to all aspects).
     */
    public Set<String> getRequiredAspects(String typeName, Set<String> requestedFields) {
        Set<String> aspects = new HashSet<>();

        for (String field : requestedFields) {
            // Skip introspection and special fields
            if (field.startsWith("__") || field.equals("urn") || field.equals("type")) {
                continue;
            }

            String key = typeName + "." + field;
            Set<String> fieldAspects = fieldToAspects.get(key);

            if (fieldAspects != null) {
                aspects.addAll(fieldAspects);
            } else {
                // Unmapped field - fallback to all aspects
                log.debug("Field {} has no @aspectMapping directive, will fetch all aspects", key);
                return null;
            }
        }

        return aspects.isEmpty() ? Collections.emptySet() : aspects;
    }
}
