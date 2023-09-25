package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import java.util.Collections;


/**
 * Provides field resolver for entity urn given entitySpec
 */
public class EntityUrnFieldResolverProvider implements EntityFieldResolverProvider {
  @Override
  public EntityFieldType getFieldType() {
    return EntityFieldType.URN;
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromValues(Collections.singleton(entitySpec.getEntity()));
  }
}
