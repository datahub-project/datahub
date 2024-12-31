package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.BooleanBox;
import com.linkedin.datahub.graphql.generated.FloatBox;
import com.linkedin.datahub.graphql.generated.IntBox;
import com.linkedin.datahub.graphql.generated.StringBox;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class HyperParameterValueTypeResolver implements TypeResolver {

  public static final String STRING_BOX = "StringBox";
  public static final String INT_BOX = "IntBox";
  public static final String FLOAT_BOX = "FloatBox";
  public static final String BOOLEAN_BOX = "BooleanBox";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof StringBox) {
      return env.getSchema().getObjectType(STRING_BOX);
    } else if (env.getObject() instanceof IntBox) {
      return env.getSchema().getObjectType(INT_BOX);
    } else if (env.getObject() instanceof BooleanBox) {
      return env.getSchema().getObjectType(BOOLEAN_BOX);
    } else if (env.getObject() instanceof FloatBox) {
      return env.getSchema().getObjectType(FLOAT_BOX);
    } else {
      throw new RuntimeException(
          "Unrecognized object type provided to type resolver, Type:" + env.getObject().toString());
    }
  }
}
