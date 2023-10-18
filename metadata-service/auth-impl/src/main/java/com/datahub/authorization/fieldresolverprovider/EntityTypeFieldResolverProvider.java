package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import java.util.Collections;


/**
 * Provides field resolver for entity type given entitySpec
 */
public class EntityTypeFieldResolverProvider implements EntityFieldResolverProvider {
  @Override
  public EntityFieldType getFieldType() {
    return EntityFieldType.TYPE;
  }

  @Override
  public FieldResolver getFieldResolver(EntitySpec entitySpec) {
    return FieldResolver.getResolverFromValues(Collections.singleton(entitySpec.getType()));
  }
}
