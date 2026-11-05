package com.linkedin.datahub.graphql.resolvers.type;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

/**
 * Resolves an interface to the object type whose name matches the simple name of the generated Java
 * class backing the value, e.g. {@code Dataset} or {@code DatasetProperties}.
 */
public class ClassNameTypeResolver implements TypeResolver {

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    return env.getSchema().getObjectType(env.getObject().getClass().getSimpleName());
  }
}
