package com.linkedin.datahub.graphql.resolvers.type;

import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpUser;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class ResolvedActorResolver implements TypeResolver {

  public static final String CORP_USER = "CorpUser";
  public static final String CORP_GROUP = "CorpGroup";

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    if (env.getObject() instanceof CorpUser) {
      return env.getSchema().getObjectType(CORP_USER);
    } else if (env.getObject() instanceof CorpGroup) {
      return env.getSchema().getObjectType(CORP_GROUP);
    } else {
      throw new RuntimeException(
          "Unrecognized object type provided to type resolver, Type:" + env.getObject().toString());
    }
  }
}
