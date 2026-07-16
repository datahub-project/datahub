package com.linkedin.metadata.aspect.plugins.validation;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.PluginSpec;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Plugin base class for aspect payload validators.
 *
 * <p>Validators run during write-time aspect mutation. They receive an {@link OperationFingerprint}
 * — the narrow, entity-registry-side view over the request's {@code OperationContext} — so they can
 * read actor / request-id / tenant-routing data without pulling the full {@code OperationContext}
 * type (and the heavy retriever world it drags in) onto entity-registry's classpath. Call sites in
 * higher modules pass the full {@code OperationContext} transparently because it implements {@link
 * OperationFingerprint}.
 *
 * <p>For per-aspect lookups during validation use the {@link OperationFingerprint}-taking overloads
 * on {@link com.linkedin.metadata.aspect.AspectRetriever} (obtainable from {@link
 * RetrieverContext#getAspectRetriever()}) — they route correctly under multi-tenant deployments.
 */
public abstract class AspectPayloadValidator extends PluginSpec {

  /**
   * Validate a proposal for the given change type for an aspect within the context of the given
   * entity's urn.
   *
   * @return whether the aspect proposal is valid
   */
  public final Stream<AspectValidationException> validateProposed(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return validateProposedAspects(
        operationContext,
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
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return validatePreCommitAspects(
        operationContext,
        changeMCPs.stream()
            .filter(i -> shouldApply(i.getChangeType(), i.getUrn(), i.getAspectName()))
            .collect(Collectors.toList()),
        retrieverContext);
  }

  protected abstract Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext);

  protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return Stream.empty();
  }

  private Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return Stream.concat(
        validateProposedAspects(operationContext, mcpItems, retrieverContext),
        validateProposedAspectsWithAuth(operationContext, mcpItems, retrieverContext, session));
  }

  protected abstract Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext);
}
