package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
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
public class ModifyIngestionSourceAuthValidator extends AspectPayloadValidator {
  private static final String SCHEDULE_DENIED_MESSAGE =
      "Doesn't have permissions to schedule this Ingestion source";

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
    Map<Urn, DataHubIngestionSourceInfo> stored =
        PatchItemImpl.readStoredForPatches(
            operationContext,
            aspectRetriever,
            mcpItems,
            INGESTION_INFO_ASPECT_NAME,
            DataHubIngestionSourceInfo.class);
    for (BatchItem item : mcpItems) {
      ChangeType type = item.getChangeType();

      switch (type) {
        case CREATE:
        case CREATE_ENTITY:
          if (!AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.CREATE, List.of(item.getUrn()))) {
            exceptions.addAuthException(
                item, "Doesn't have permissions to create this Ingestion source");
            continue;
          }
          break;
        case RESTATE:
        case PATCH:
        case UPDATE:
        case UPSERT:
          if (!AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.UPDATE, List.of(item.getUrn()))) {
            exceptions.addAuthException(
                item, "Doesn't have permissions to edit this Ingestion source");
            continue;
          }
          break;
        case DELETE:
          if (!AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.DELETE, List.of(item.getUrn()))) {
            exceptions.addAuthException(
                item, "Doesn't have permissions to delete this Ingestion source");
          }
          stored.remove(item.getUrn());
          continue;
        default:
          throw new RuntimeException("Unsupported change type: " + type);
      }

      // A scheduled source is executed by the scheduler as the system actor, so a user who can only
      // edit it would otherwise get recurring execution. Leaving a schedule on the source (or
      // editing a scheduled source) requires permission to execute it. This is a fast fail on the
      // aspect read here; validatePreCommitAspectsWithAuth repeats it on the aspect being written.
      final DataHubIngestionSourceInfo proposed;
      try {
        proposed =
            PatchItemImpl.resolveProposedAspect(
                item, stored.get(item.getUrn()), aspectRetriever, DataHubIngestionSourceInfo.class);
      } catch (RuntimeException e) {
        // dataHubIngestionSourceInfo has no patch template; a PATCH that cannot be applied fails
        // the write too.
        log.warn("Unable to resolve dataHubIngestionSourceInfo PATCH for {}", item.getUrn(), e);
        exceptions.addException(item, "Unable to apply dataHubIngestionSourceInfo PATCH", e);
        continue;
      }
      if (proposed != null && proposed.hasSchedule() && !canExecute(session, item.getUrn())) {
        exceptions.addAuthException(item, SCHEDULE_DENIED_MESSAGE);
        continue;
      }
      // Items are applied in order, so a later PATCH to the same source applies on top of this one.
      if (proposed != null) {
        stored.put(item.getUrn(), proposed);
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

  /**
   * Repeats the schedule check on the aspect about to be written. A PATCH is applied again to the
   * latest stored aspect inside the transaction, which may carry a schedule written after the
   * proposal was validated.
   */
  @Override
  protected Stream<AspectValidationException> validatePreCommitAspectsWithAuth(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {
    if (session == null) {
      return Stream.empty();
    }

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    for (ChangeMCP item : changeMCPs) {
      if (ChangeType.DELETE.equals(item.getChangeType())) {
        continue;
      }
      DataHubIngestionSourceInfo info = item.getAspect(DataHubIngestionSourceInfo.class);
      if (info != null && info.hasSchedule() && !canExecute(session, item.getUrn())) {
        exceptions.addAuthException(item, SCHEDULE_DENIED_MESSAGE);
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static boolean canExecute(@Nonnull AuthorizationSession session, @Nonnull Urn urn) {
    return AuthUtil.isAuthorizedEntityUrns(session, ApiOperation.EXECUTE, List.of(urn));
  }
}
