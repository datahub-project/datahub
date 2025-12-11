/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
