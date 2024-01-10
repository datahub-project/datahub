package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.common.AuditStamp;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Applies changes to the RecordTemplate prior to write */
public abstract class MutationHook extends PluginSpec {

  public MutationHook(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  /**
   * Mutating hook
   *
   * @param changeType Type of change to mutate
   * @param entitySpec Entity specification
   * @param aspectSpec Aspect specification
   * @param oldAspectValue old aspect vale if it exists
   * @param newAspectValue the new aspect
   * @param oldSystemMetadata old system metadata if it exists
   * @param newSystemMetadata the new system metadata
   * @param auditStamp the audit stamp
   */
  public final void applyMutation(
      @Nonnull final ChangeType changeType,
      @Nonnull EntitySpec entitySpec,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull AspectRetriever aspectRetriever) {
    if (shouldApply(changeType, entitySpec.getName(), aspectSpec)) {
      mutate(
          changeType,
          entitySpec,
          aspectSpec,
          oldAspectValue,
          newAspectValue,
          oldSystemMetadata,
          newSystemMetadata,
          auditStamp,
          aspectRetriever);
    }
  }

  protected abstract void mutate(
      @Nonnull final ChangeType changeType,
      @Nonnull EntitySpec entitySpec,
      @Nonnull final AspectSpec aspectSpec,
      @Nullable final RecordTemplate oldAspectValue,
      @Nullable final RecordTemplate newAspectValue,
      @Nullable final SystemMetadata oldSystemMetadata,
      @Nullable final SystemMetadata newSystemMetadata,
      @Nonnull AuditStamp auditStamp,
      @Nonnull AspectRetriever aspectRetriever);
}
