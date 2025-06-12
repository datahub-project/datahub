package com.linkedin.metadata.ingestion.validation;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.authorization.ApiOperation;
import java.util.Collection;
import java.util.List;
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
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspectsWithAuth(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nullable AuthorizationSession session) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    if (session == null) {
      log.warn("Authentication details not found, ignoring...");
      return Stream.empty();
    }

    for (BatchItem item : mcpItems) {
      ChangeType type = item.getChangeType();

      switch (type) {
        case CREATE:
        case CREATE_ENTITY:
          if (!AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.CREATE, List.of(item.getUrn()))) {
            exceptions.addAuthException(
                item, "Doesn't have permissions to create this Ingestion source");
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
          }
          break;
        case DELETE:
          if (!AuthUtil.isAuthorizedEntityUrns(
              session, ApiOperation.DELETE, List.of(item.getUrn()))) {
            exceptions.addAuthException(
                item, "Doesn't have permissions to delete this Ingestion source");
          }
          break;
        default:
          throw new RuntimeException("Unsupported change type: " + type);
      }
    }

    return exceptions.streamAllExceptions();
  }

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
