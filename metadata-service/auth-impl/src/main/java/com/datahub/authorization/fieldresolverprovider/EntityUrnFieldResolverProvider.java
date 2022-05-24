package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResourceFieldType;
import com.datahub.authorization.ResourceSpec;
import java.util.Collections;


/**
 * Provides field resolver for entity urn given resourceSpec
 */
public class EntityUrnFieldResolverProvider implements ResourceFieldResolverProvider {
  @Override
  public ResourceFieldType getFieldType() {
    return ResourceFieldType.RESOURCE_URN;
  }

  @Override
  public FieldResolver getFieldResolver(ResourceSpec resourceSpec) {
    return FieldResolver.getResolverFromValues(Collections.singleton(resourceSpec.getResource()));
  }
}
