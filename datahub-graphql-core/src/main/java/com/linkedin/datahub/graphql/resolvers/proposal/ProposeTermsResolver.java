package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrn;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrns;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ProposeTermsInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.EntityDoesNotExistException;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposeTermsResolver implements DataFetcher<CompletableFuture<String>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Bind Inputs
    final ProposeTermsInput input =
        bindArgument(environment.getArgument("input"), ProposeTermsInput.class);
    final List<String> termUrnStrs = input.getTermUrns();
    final String targetUrnStr = input.getResourceUrn();
    final String description = input.getDescription();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Step 1: validate and authorize the action
          validateAndAuthorize(context, termUrnStrs, targetUrnStr, input);

          // Step 2: raise the proposal.
          try {
            if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
              // Propose Schema Field Terms
              return this.actionRequestService
                  .proposeSchemaFieldTerms(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      input.getSubResource(),
                      toUrns(termUrnStrs),
                      description)
                  .toString();
            } else {
              // Propose Asset Terms
              return this.actionRequestService
                  .proposeEntityTerms(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      toUrns(termUrnStrs),
                      description)
                  .toString();
            }
          } catch (Exception e) {
            if (e instanceof ActionRequestService.MalformedActionRequestException) {
              throw new DataHubGraphQLException(
                  "Failed to propose terms: " + e.getMessage(),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyAppliedException) {
              throw new DataHubGraphQLException(
                  "Terms are already applied to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyRequestedException) {
              throw new DataHubGraphQLException(
                  "Terms have already been proposed to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof RemoteInvocationException) {
              throw new DataHubGraphQLException(
                  "Failed to create term proposal. Encountered an error while attempting to reach the downstream service: "
                      + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            } else if (e instanceof EntityDoesNotExistException) {
              throw new DataHubGraphQLException(
                  "Failed to create term proposal: " + e.getMessage(),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            } else {
              throw new DataHubGraphQLException(
                  "Failed to propose terms: " + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR,
                  e);
            }
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateAndAuthorize(
      QueryContext context,
      List<String> termUrnStrs,
      String targetUrnStr,
      ProposeTermsInput input) {
    try {
      toUrns(termUrnStrs);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid term urns provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    try {
      toUrn(targetUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid resource urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!ProposalUtils.isAuthorizedToProposeTerms(
        context, toUrn(targetUrnStr), input.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (termUrnStrs.isEmpty()) {
      throw new DataHubGraphQLException(
          "No terms provided to propose", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!termUrnStrs.stream()
        .allMatch(
            urnStr ->
                UrnUtils.getUrn(urnStr)
                    .getEntityType()
                    .equals(Constants.GLOSSARY_TERM_ENTITY_NAME))) {
      throw new DataHubGraphQLException(
          "All provided term must be of type 'glossaryTerm'", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())
        && input.getSubResource() == null) {
      throw new DataHubGraphQLException(
          "subResource field must be provided for proposing terms on dataset fields",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (input.getSubResource() != null
        && !SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
      throw new DataHubGraphQLException(
          "Unsupported subResourceType type 'none' provided", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }
}
