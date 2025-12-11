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

/**
 * Responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Aspect} interface
 * type.
 */
public class AspectInterfaceTypeResolver implements TypeResolver {

  public AspectInterfaceTypeResolver() {}

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    // TODO(Gabe): Fill this out. This method is not called today. We will need to fill this
    // out in the case we ever want to return fields of type Aspect in graphql. Right now
    // we just use Aspect to define the shared `version` field.
    return null;
  }
}
