package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public abstract class AspectPayloadValidator extends PluginSpec {

  /**
   * Validate a proposal for the given change type for an aspect within the context of the given
   * entity's urn.
   *
   * @return whether the aspect proposal is valid
   */
  public final Stream<AspectValidationException> validateProposed(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return validateProposedAspects(
        mcpItems.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getUrn(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  /**
   * Validate the proposed aspect as its about to be written with the context of the previous
   * version of the aspect (if it existed)
   *
   * @return whether the aspect proposal is valid
   */
  public final Stream<AspectValidationException> validatePreCommit(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return validatePreCommitAspects(
        changeMCPs.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getUrn(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  protected abstract Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext);

  protected abstract Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext);
}
