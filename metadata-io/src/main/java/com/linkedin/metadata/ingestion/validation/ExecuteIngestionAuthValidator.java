package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Setter
@Getter
@Slf4j
@Accessors(chain = true)
public class ExecuteIngestionAuthValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    if (session == null) {
      log.warn("Authentication details not found, ignoring...");
      return Stream.empty();
    }

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    Map<Urn, ExecutionRequestInput> stored =
        PatchItemImpl.readStoredForPatches(
            operationContext,
            aspectRetriever,
            mcpItems,
            EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            ExecutionRequestInput.class);
    for (BatchItem item : mcpItems) {
      final ExecutionRequestInput executionRequestInput;
      try {
        executionRequestInput =
            PatchItemImpl.resolveProposedAspect(
                item, stored.get(item.getUrn()), aspectRetriever, ExecutionRequestInput.class);
      } catch (RuntimeException e) {
        // executionRequestInput has no patch template; a PATCH that cannot be applied fails the
        // write too.
        exceptions.addAuthException(
            item, "Unable to resolve executionRequestInput PATCH for authorization");
        continue;
      }
      // hasSource guards the dereference only. source is required by the PDL and
      // ChangeItemImpl.build validates the patched record, so a sourceless patch is already
      // rejected above as unresolvable; this keeps an auth validator from throwing if that ever
      // stops holding.
      final Urn ingestionSourceUrn =
          executionRequestInput != null && executionRequestInput.hasSource()
              ? executionRequestInput.getSource().getIngestionSource()
              : null;

      // validate explicitly only for ingestion sources - if you have permission to execute it
      if (ingestionSourceUrn != null
          && !AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.EXECUTE, List.of(ingestionSourceUrn))) {
        exceptions.addAuthException(item, "Doesn't have permissions to execute this Ingestion");
      }
    }

    return exceptions.streamAllExceptions();
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
