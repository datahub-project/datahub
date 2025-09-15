package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for dismissing user suggestions to prevent them from being recommended again. */
@Slf4j
public class DismissUserSuggestionResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;
  private final EntityService<?> _entityService;

  public DismissUserSuggestionResolver(
      final EntityClient entityClient, final EntityService<?> entityService) {
    _entityClient = entityClient;
    _entityService = entityService;
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String userUrn = bindArgument(environment.getArgument("userUrn"), String.class);

          log.info("User {} dismissing suggestion for user {}", context.getActorUrn(), userUrn);

          if (!AuthorizationUtils.canManageUsersAndGroups(context)) {
            throw new AuthorizationException(
                "Unauthorized to dismiss user suggestions. Please contact your DataHub administrator.");
          }

          try {
            return dismissSuggestion(context, userUrn);
          } catch (Exception e) {
            log.error("Failed to dismiss suggestion for user: {}", userUrn, e);
            throw new RuntimeException("Failed to dismiss user suggestion", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean dismissSuggestion(final QueryContext context, final String userUrn)
      throws Exception {
    CorpuserUrn corpUserUrn = CorpuserUrn.createFromString(userUrn);

    // Check if invitation status already exists
    EntityResponse entityResponse =
        _entityClient.getV2(
            context.getOperationContext(),
            Constants.CORP_USER_ENTITY_NAME,
            corpUserUrn,
            Set.of(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn(context.getActorUrn()));

    CorpUserInvitationStatus dismissedStatus = new CorpUserInvitationStatus();
    dismissedStatus.setStatus(InvitationStatus.SUGGESTION_DISMISSED);
    dismissedStatus.setInvitationToken(UUID.randomUUID().toString()); // Generate a unique token
    dismissedStatus.setLastUpdated(auditStamp);

    if (entityResponse != null
        && entityResponse
            .getAspects()
            .containsKey(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME)) {
      // Update existing invitation status
      EnvelopedAspect envelopedAspect =
          entityResponse.getAspects().get(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
      CorpUserInvitationStatus currentStatus =
          new CorpUserInvitationStatus(envelopedAspect.getValue().data());

      // Keep original creation time if it exists
      dismissedStatus.setCreated(currentStatus.getCreated());

      // Preserve role if it was set
      if (currentStatus.hasRole()) {
        dismissedStatus.setRole(currentStatus.getRole());
      }
    } else {
      // Create new invitation status with dismissed status
      dismissedStatus.setCreated(auditStamp);
    }

    // Create metadata change proposal
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(corpUserUrn);
    mcp.setEntityType(Constants.CORP_USER_ENTITY_NAME);
    mcp.setAspectName(Constants.CORP_USER_INVITATION_STATUS_ASPECT_NAME);
    mcp.setAspect(GenericRecordUtils.serializeAspect(dismissedStatus));
    mcp.setChangeType(com.linkedin.events.metadata.ChangeType.UPSERT);

    // Create AspectsBatch with single MCP
    AspectsBatchImpl aspectsBatch =
        AspectsBatchImpl.builder()
            .mcps(List.of(mcp), auditStamp, context.getOperationContext().getRetrieverContext())
            .build(context.getOperationContext());

    // Ingest the aspect
    _entityService.ingestProposal(context.getOperationContext(), aspectsBatch, false);

    log.info("Successfully dismissed suggestion for user: {}", userUrn);
    return true;
  }
}
