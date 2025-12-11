/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.type;

import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;

public class TimeSeriesAspectInterfaceTypeResolver implements TypeResolver {

  public TimeSeriesAspectInterfaceTypeResolver() {}

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    // TODO(John): Fill this out.
    return null;
  }
}
