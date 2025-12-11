/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Provides field resolver for entity urn given entitySpec */
public class EntityUrnFieldResolverProvider implements EntityFieldResolverProvider {

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return ImmutableList.of(EntityFieldType.URN, EntityFieldType.RESOURCE_URN);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getUrn);
  }

  private FieldResolver.FieldValue getUrn(EntitySpec entitySpec) {
    if (entitySpec.getEntity().isEmpty()) {
      return FieldResolver.emptyFieldValue();
    }
    return FieldResolver.FieldValue.builder().values(Set.of(entitySpec.getEntity())).build();
  }
}
