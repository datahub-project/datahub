package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.plugins.ConfigurableEntityAspectPlugin;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.AspectSpec;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public abstract class AspectPayloadValidator implements ConfigurableEntityAspectPlugin {

  private final AspectPluginConfig config;

  public AspectPayloadValidator(AspectPluginConfig config) {
    this.config = config;
  }

  public boolean shouldApply(@Nonnull String aspectName) {
    return config.isEnabled() && isAspectSupported(aspectName);
  }

  /**
   * Validate a proposal for the given change type for an aspect within the context of the given
   * entity's urn.
   *
   * @param changeType The change type
   * @param entityUrn The parent entity for the aspect
   * @param aspectSpec The aspect's specification
   * @param aspectPayload The aspect's payload
   * @return whether the aspect proposal is valid
   * @throws AspectValidationException
   */
  public final boolean validateProposed(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull RecordTemplate aspectPayload,
      AspectRetriever aspectRetriever)
      throws AspectValidationException {
    if (shouldApply(changeType, entityUrn, aspectSpec)) {
      return validateProposedAspect(
          changeType, entityUrn, aspectSpec, aspectPayload, aspectRetriever);
    }

    return true;
  }

  protected abstract boolean validateProposedAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull RecordTemplate aspectPayload,
      AspectRetriever aspectRetriever)
      throws AspectValidationException;

  /**
   * Validate the proposed aspect as its about to be written with the context of the previous
   * version of the aspect (if it existed)
   *
   * @param changeType The change type
   * @param entityUrn The parent entity for the aspect
   * @param aspectSpec The aspect's specification
   * @param previousAspect The previous version of the aspect if it exists
   * @param proposedAspect The new version of the aspect
   * @return whether the aspect proposal is valid
   * @throws AspectValidationException
   */
  public final boolean validatePreCommit(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate previousAspect,
      @Nonnull RecordTemplate proposedAspect,
      AspectRetriever aspectRetriever)
      throws AspectValidationException {
    if (shouldApply(changeType, entityUrn, aspectSpec)) {
      return validatePreCommitAspect(
          changeType, entityUrn, aspectSpec, previousAspect, proposedAspect, aspectRetriever);
    }

    return true;
  }

  protected abstract boolean validatePreCommitAspect(
      @Nonnull ChangeType changeType,
      @Nonnull Urn entityUrn,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate previousAspect,
      @Nonnull RecordTemplate proposedAspect,
      AspectRetriever aspectRetriever)
      throws AspectValidationException;
}
