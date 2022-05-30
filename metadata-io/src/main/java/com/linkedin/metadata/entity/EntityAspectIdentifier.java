package com.linkedin.metadata.entity;

import lombok.Value;

import javax.annotation.Nonnull;

/**
 * This class holds values required to construct a unique key to identify an entity aspect record in a database.
 * Its existence started mainly for compatibility with {@link com.linkedin.metadata.entity.ebean.EbeanAspectV2.PrimaryKey}
 */
@Value
public class EntityAspectIdentifier {
  @Nonnull String urn;
  @Nonnull String aspect;
  long version;
}
