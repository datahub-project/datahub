package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.KeyValueSchema;
import com.linkedin.datahub.graphql.generated.TableSchema;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class PlatformSchemaUnionTypeResolver implements TypeResolver {

  private static final String TABLE_SCHEMA_TYPE_NAME = "TableSchema";
  private static final String KEY_VALUE_SCHEMA_TYPE_NAME = "KeyValueSchema";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof TableSchema) {
      return env.getSchema().getObjectType(TABLE_SCHEMA_TYPE_NAME);
    } else if (env.getObject() instanceof KeyValueSchema) {
      return env.getSchema().getObjectType(KEY_VALUE_SCHEMA_TYPE_NAME);
    } else {
      throw new RuntimeException("Unrecognized object type provided to type resolver");
    }
  }
}
