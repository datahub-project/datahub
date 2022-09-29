package com.datahub.auth.authorization.fieldresolverprovider;

import com.datahub.plugins.auth.authorization.FieldResolver;
import com.datahub.plugins.auth.authorization.ResourceFieldType;
import com.datahub.plugins.auth.authorization.ResourceSpec;


/**
 * Base class for defining a class that provides the field resolver for the given field type
 */
public interface ResourceFieldResolverProvider {

  /**
   * Field that this hydrator is hydrating
   */
  ResourceFieldType getFieldType();

  /**
   * Return resolver for fetching the field values given the resource
   */
  FieldResolver getFieldResolver(ResourceSpec resourceSpec);
}
