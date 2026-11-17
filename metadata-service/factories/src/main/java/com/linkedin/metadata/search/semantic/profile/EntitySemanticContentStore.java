package com.linkedin.metadata.search.semantic.profile;

import static com.linkedin.metadata.Constants.SEMANTIC_CONTENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.SemanticContent;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.template.common.SemanticProfileConflictException;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Reads {@code semanticContent} through the operation's {@link AspectRetriever}. The profile patch
 * is ingested through {@link EntityService}, which the retriever does not provide. That write is
 * wired only where an entity service exists, not from the shared plugin configuration.
 */
@Slf4j
public class EntitySemanticContentStore implements SemanticContentStore {

  private final EntityService<?> entityService;

  public EntitySemanticContentStore(@Nonnull EntityService<?> entityService) {
    this.entityService = entityService;
  }

  @Override
  public boolean exists(@Nonnull OperationContext context, @Nonnull Urn urn) {
    return Boolean.TRUE.equals(
        context.getAspectRetriever().entityExists(context, Set.of(urn)).get(urn));
  }

  @Nullable
  @Override
  public SemanticContent get(@Nonnull OperationContext context, @Nonnull Urn urn) {
    Aspect aspect =
        context
            .getAspectRetriever()
            .getLatestAspectObject(context, urn, SEMANTIC_CONTENT_ASPECT_NAME);
    if (aspect == null || aspect.data() == null) {
      return null;
    }
    try {
      return new SemanticContent(aspect.data().copy());
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException("Failed to copy semanticContent for " + urn, e);
    }
  }

  @Override
  public boolean patch(
      @Nonnull OperationContext context, @Nonnull Urn urn, @Nonnull MetadataChangeProposal patch) {
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    IngestResult result;
    try {
      result = entityService.ingestProposal(systemIngestContext(context), patch, auditStamp, false);
    } catch (RuntimeException e) {
      if (isProfileConflict(e)) {
        return false;
      }
      throw e;
    }
    if (result == null || !result.isSqlCommitted()) {
      log.warn("semanticContent patch for {} was not committed", urn);
      return false;
    }
    return true;
  }

  /**
   * A non-system writer loses {@code semanticApply} before commit. Apply is a service write, so the
   * ingest runs as the context's system actor. Reads stay on the caller context.
   */
  @Nonnull
  private static OperationContext systemIngestContext(@Nonnull OperationContext context) {
    if (context.isSystemAuth()) {
      return context;
    }
    Authentication systemAuthentication =
        context
            .getSystemAuthentication()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "System authentication is required to apply semanticContent"));
    return context.toBuilder()
        .operationContextConfig(
            context.getOperationContextConfig().toBuilder().allowSystemAuthentication(true).build())
        .build(systemAuthentication, context.getSessionActorContext().isEnforceExistenceEnabled());
  }

  private static boolean isProfileConflict(@Nonnull Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof SemanticProfileConflictException) {
        return true;
      }
      if (cause.getCause() == cause) {
        return false;
      }
    }
    return false;
  }
}
