package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.NumberValue;
import com.linkedin.datahub.graphql.generated.StringValue;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class PropertyValueResolver implements TypeResolver {

  public static final String STRING_VALUE = "StringValue";
  public static final String NUMBER_VALUE = "NumberValue";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof StringValue) {
      return env.getSchema().getObjectType(STRING_VALUE);
    } else if (env.getObject() instanceof NumberValue) {
      return env.getSchema().getObjectType(NUMBER_VALUE);
    } else {
      throw new RuntimeException(
          "Unrecognized object type provided to type resolver, Type:" + env.getObject().toString());
    }
  }
}
