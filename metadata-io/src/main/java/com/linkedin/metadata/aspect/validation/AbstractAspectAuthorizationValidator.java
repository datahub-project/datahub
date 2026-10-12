package com.linkedin.metadata.aspect.validation;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Base class for aspect write authorization validators with shared source/session filtering. */
@Slf4j
public abstract class AbstractAspectAuthorizationValidator extends AspectPayloadValidator {

  @Override
  protected final Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    if (mcpItems.isEmpty()) {
      return exceptions.streamAllExceptions();
    }

    // Trusted system principals skip aspect auth entirely. Never trust client-writable appSource.
    if (isTrustedSystemOperation(operationContext, session)) {
      return exceptions.streamAllExceptions();
    }

    List<? extends BatchItem> itemsRequiringValidation = new ArrayList<>(mcpItems);

    if (session == null) {
      exceptions.addException(
          itemsRequiringValidation.stream().findFirst().orElseThrow(IllegalStateException::new),
          "No authentication details found, cannot authorize change.");
      return exceptions.streamAllExceptions();
    }

    validateItems(operationContext, itemsRequiringValidation, mcpItems, retrieverContext, session)
        .forEach(exceptions::addException);

    return exceptions.streamAllExceptions();
  }

  /**
   * Trusted when the operation fingerprint or authorization session is the configured system
   * principal. Client-writable provenance (e.g. {@code appSource}) must never gate this decision. A
   * null fingerprint (e.g. {@code AspectsBatchImpl.build(null)}) is untrusted.
   */
  protected static boolean isTrustedSystemOperation(
      @Nullable OperationFingerprint operationContext, @Nullable AuthorizationSession session) {
    if (operationContext != null && operationContext.isSystemAuth()) {
      return true;
    }
    return session instanceof OperationContext && ((OperationContext) session).isSystemAuth();
  }

  protected abstract List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session);

  protected AspectValidationException authFailure(
      @Nonnull BatchItem item, @Nonnull String message) {
    return AspectValidationException.forAuth(item, message);
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    return Stream.empty();
  }
}
