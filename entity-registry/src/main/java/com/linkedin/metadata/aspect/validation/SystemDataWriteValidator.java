package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocks writes to system entities unless the request session actor is {@link
 * com.linkedin.metadata.Constants#SYSTEM_ACTOR}. Uses {@link OperationFingerprint#getAuditStamp()}
 * (session identity) rather than the proposal audit stamp, which callers can set arbitrarily.
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class SystemDataWriteValidator extends AspectPayloadValidator {

  private static final String VALIDATION_MESSAGE =
      "System data entities may only be modified by the system actor.";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        item -> {
          if (item.getEntitySpec().isSystemEntity() && !isSystemActor(operationContext)) {
            exceptions.addException(AspectValidationException.forItem(item, VALIDATION_MESSAGE));
          }
        });

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  private static boolean isSystemActor(@Nonnull final OperationFingerprint operationContext) {
    if (operationContext.getAuditStamp() == null
        || operationContext.getAuditStamp().getActor() == null) {
      return false;
    }
    return SYSTEM_ACTOR.equals(operationContext.getAuditStamp().getActor().toString());
  }
}
