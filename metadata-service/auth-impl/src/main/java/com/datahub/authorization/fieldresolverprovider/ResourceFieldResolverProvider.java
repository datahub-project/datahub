package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResourceSpec;


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
