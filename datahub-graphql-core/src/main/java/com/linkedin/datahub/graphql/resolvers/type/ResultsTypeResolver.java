package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.StringBox;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class ResultsTypeResolver implements TypeResolver {

  public static final String STRING_BOX = "StringBox";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof StringBox) {
      return env.getSchema().getObjectType(STRING_BOX);
    } else {
      throw new RuntimeException(
          "Unrecognized object type provided to type resolver, Type:" + env.getObject().toString());
    }
  }
}
