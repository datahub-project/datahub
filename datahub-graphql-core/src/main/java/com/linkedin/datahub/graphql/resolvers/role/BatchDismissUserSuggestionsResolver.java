package com.linkedin.datahub.graphql.resolvers.role;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Resolver for batch dismissing user suggestions to prevent them from being recommended again. */
@Slf4j
public class BatchDismissUserSuggestionsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final UserSuggestionDismissalService _dismissalService;

  public BatchDismissUserSuggestionsResolver(
      final EntityClient entityClient, final EntityService<?> entityService) {
    _dismissalService = new UserSuggestionDismissalService(entityClient, entityService);
  }

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          @SuppressWarnings("unchecked")
          final Map<String, Object> inputMap =
              (Map<String, Object>) environment.getArgument("input");
          @SuppressWarnings("unchecked")
          final List<String> userUrns = (List<String>) inputMap.get("userUrns");

          log.info(
              "User {} batch dismissing suggestions for {} users",
              context.getActorUrn(),
              userUrns.size());

          _dismissalService.validateAuthorization(context);

          try {
            return batchDismissSuggestions(context, userUrns);
          } catch (Exception e) {
            log.error("Failed to batch dismiss suggestions for users: {}", userUrns, e);
            throw new RuntimeException("Failed to batch dismiss user suggestions", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Boolean batchDismissSuggestions(final QueryContext context, final List<String> userUrns)
      throws Exception {

    List<CorpuserUrn> corpUserUrns = new ArrayList<>();
    for (String userUrn : userUrns) {
      corpUserUrns.add(CorpuserUrn.createFromString(userUrn));
    }

    // Get existing invitation statuses for all users in batch
    Map<Urn, EntityResponse> existingStatuses =
        _dismissalService.getExistingInvitationStatuses(
            context, new java.util.HashSet<>(corpUserUrns));

    AuditStamp auditStamp = _dismissalService.createAuditStamp(context);
    List<MetadataChangeProposal> mcps = new ArrayList<>();

    for (CorpuserUrn corpUserUrn : corpUserUrns) {
      CorpUserInvitationStatus existingStatus =
          _dismissalService.extractInvitationStatus(existingStatuses.get(corpUserUrn));
      CorpUserInvitationStatus dismissedStatus =
          _dismissalService.createDismissedStatus(auditStamp, existingStatus);
      MetadataChangeProposal mcp =
          _dismissalService.createMetadataChangeProposal(corpUserUrn, dismissedStatus);
      mcps.add(mcp);
    }

    _dismissalService.ingestDismissals(context, mcps, auditStamp);

    log.info("Successfully batch dismissed suggestions for {} users", userUrns.size());
    return true;
  }
}
