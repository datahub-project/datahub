package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrn;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ProposeDomainInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.EntityDoesNotExistException;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for proposing a single domain. */
@Slf4j
@RequiredArgsConstructor
public class ProposeDomainResolver implements DataFetcher<CompletableFuture<String>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Bind Inputs
    final ProposeDomainInput input =
        bindArgument(environment.getArgument("input"), ProposeDomainInput.class);
    final String domainUrnStr = input.getDomainUrn();
    final String targetUrnStr = input.getResourceUrn();
    final String description = input.getDescription();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Step 1: validate and authorize the action
          validateAndAuthorize(context, domainUrnStr, targetUrnStr);

          // Step 2: raise the proposal.
          try {
            // Propose Asset Domain
            return this.actionRequestService
                .proposeEntityDomain(
                    context.getOperationContext(),
                    toUrn(targetUrnStr),
                    toUrn(domainUrnStr),
                    description)
                .toString();
          } catch (Exception e) {
            if (e instanceof ActionRequestService.MalformedActionRequestException) {
              throw new DataHubGraphQLException(
                  "Failed to propose domain: " + e.getMessage(),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyAppliedException) {
              throw new DataHubGraphQLException(
                  "Domain is already applied to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyRequestedException) {
              throw new DataHubGraphQLException(
                  "Domain has already been proposed to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof RemoteInvocationException) {
              throw new DataHubGraphQLException(
                  "Failed to create domain proposal. Encountered an error while attempting to reach the downstream service: "
                      + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            } else if (e instanceof EntityDoesNotExistException) {
              throw new DataHubGraphQLException(
                  "Failed to create Domain proposal: " + e.getMessage(),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            } else {
              throw new DataHubGraphQLException(
                  "Failed to propose Domain: " + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR,
                  e);
            }
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateAndAuthorize(
      QueryContext context, String domainUrnStr, String targetUrnStr) {
    try {
      toUrn(domainUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid domain urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    try {
      toUrn(targetUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid resource urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!ProposalUtils.isAuthorizedToProposeDomains(context, toUrn(targetUrnStr))) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (!UrnUtils.getUrn(domainUrnStr).getEntityType().equals(Constants.DOMAIN_ENTITY_NAME)) {
      throw new DataHubGraphQLException(
          "All provided domain must be of type 'domain'", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }
}
