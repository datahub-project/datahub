package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;


/**
 * Base class for defining a class that provides the field resolver for the given field type
 */
public interface EntityFieldResolverProvider {

  /**
   * Field that this hydrator is hydrating
   */
  EntityFieldType getFieldType();

  /**
   * Return resolver for fetching the field values given the entity
   */
  FieldResolver getFieldResolver(EntitySpec entitySpec);
}
