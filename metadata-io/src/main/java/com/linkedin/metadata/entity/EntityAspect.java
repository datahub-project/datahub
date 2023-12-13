package com.linkedin.metadata.entity;

import java.sql.Timestamp;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * This is an internal representation of an entity aspect record {@link EntityServiceImpl} and
 * {@link AspectDao} implementations are using. While {@link AspectDao} implementations have their
 * own aspect record implementations, they cary implementation details that should not leak outside.
 * Therefore, this is the type to use in public {@link AspectDao} methods.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class EntityAspect {

  @Nonnull private String urn;

  @Nonnull private String aspect;

  private long version;

  private String metadata;

  private String systemMetadata;

  private Timestamp createdOn;

  private String createdBy;

  private String createdFor;

  public EntityAspectIdentifier toAspectIdentifier() {
    return new EntityAspectIdentifier(getUrn(), getAspect(), getVersion());
  }
}
