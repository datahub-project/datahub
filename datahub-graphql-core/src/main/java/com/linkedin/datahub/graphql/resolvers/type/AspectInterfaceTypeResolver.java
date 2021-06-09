package com.linkedin.datahub.graphql.resolvers.type;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

/**
 * Responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Entity} interface type.
 */
public class AspectInterfaceTypeResolver implements TypeResolver {

    public AspectInterfaceTypeResolver() { }

    @Override
    public GraphQLObjectType getType(TypeResolutionEnvironment env) {
      String fieldName = env.getField().getName();
      String typeName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
      return env.getSchema().getObjectType(typeName);
    }
}
