/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.plugins.validation;

import com.datahub.authorization.AuthorizationSession;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class AspectPayloadValidator extends PluginSpec {

  /**
   * Validate a proposal for the given change type for an aspect within the context of the given
   * entity's urn.
   *
   * @return whether the aspect proposal is valid
   */
  public final Stream<AspectValidationException> validateProposed(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return validateProposedAspects(
        mcpItems.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getUrn(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext,
        session);
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

  protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return Stream.empty();
  }

  private Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return Stream.concat(
        validateProposedAspects(mcpItems, retrieverContext),
        validateProposedAspectsWithAuth(mcpItems, retrieverContext, session));
  }

  protected abstract Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext);
}
