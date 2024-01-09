package com.linkedin.metadata.aspect.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;

public interface BatchItem {
  /**
   * The urn associated with the aspect
   *
   * @return
   */
  Urn getUrn();

  /**
   * Aspect's name
   *
   * @return the name
   */
  @Nonnull
  default String getAspectName() {
    return getAspectSpec().getName();
  }

  /**
   * System information
   *
   * @return the system metadata
   */
  SystemMetadata getSystemMetadata();

  /**
   * Timestamp and actor
   *
   * @return the audit information
   */
  AuditStamp getAuditStamp();

  /**
   * The type of change
   *
   * @return change type
   */
  @Nonnull
  ChangeType getChangeType();

  /**
   * The entity's schema
   *
   * @return entity specification
   */
  @Nonnull
  EntitySpec getEntitySpec();

  /**
   * The aspect's schema
   *
   * @return aspect's specification
   */
  @Nonnull
  AspectSpec getAspectSpec();
}
