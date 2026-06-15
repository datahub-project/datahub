package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
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

    List<? extends BatchItem> itemsRequiringValidation = filterItemsRequiringValidation(mcpItems);

    if (itemsRequiringValidation.isEmpty()) {
      return exceptions.streamAllExceptions();
    }

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

  protected List<? extends BatchItem> filterItemsRequiringValidation(
      @Nonnull Collection<? extends BatchItem> mcpItems) {
    return mcpItems.stream()
        .filter(AbstractAspectAuthorizationValidator::requiresValidation)
        .collect(Collectors.toList());
  }

  /** Skip system upgrade sources; all other sources including UI are validated. */
  protected static boolean requiresValidation(@Nonnull BatchItem item) {
    String appSource =
        item.getSystemMetadata() != null && item.getSystemMetadata().getProperties() != null
            ? item.getSystemMetadata().getProperties().get(APP_SOURCE)
            : null;
    return !SYSTEM_UPDATE_SOURCE.equals(appSource);
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
}
