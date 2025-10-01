package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for dismissing user suggestions to prevent them from being recommended again. */
@Slf4j
public class DismissUserSuggestionResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final UserSuggestionDismissalService _dismissalService;

  public DismissUserSuggestionResolver(
      final EntityClient entityClient, final EntityService<?> entityService) {
    _dismissalService = new UserSuggestionDismissalService(entityClient, entityService);
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String userUrn = bindArgument(environment.getArgument("userUrn"), String.class);

          log.info("User {} dismissing suggestion for user {}", context.getActorUrn(), userUrn);

          _dismissalService.validateAuthorization(context);

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

    AuditStamp auditStamp = _dismissalService.createAuditStamp(context);
    CorpUserInvitationStatus existingStatus =
        _dismissalService.getExistingInvitationStatus(context, corpUserUrn);
    CorpUserInvitationStatus dismissedStatus =
        _dismissalService.createDismissedStatus(auditStamp, existingStatus);
    MetadataChangeProposal mcp =
        _dismissalService.createMetadataChangeProposal(corpUserUrn, dismissedStatus);

    _dismissalService.ingestDismissals(context, List.of(mcp), auditStamp);

    log.info("Successfully dismissed suggestion for user: {}", userUrn);
    return true;
  }
}
