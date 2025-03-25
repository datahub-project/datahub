package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrn;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrns;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ProposeTagsInput;
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
public class ProposeTagsResolver implements DataFetcher<CompletableFuture<String>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Bind Inputs
    final ProposeTagsInput input =
        bindArgument(environment.getArgument("input"), ProposeTagsInput.class);
    final List<String> tagUrnStrs = input.getTagUrns();
    final String targetUrnStr = input.getResourceUrn();
    final String description = input.getDescription();

    return CompletableFuture.supplyAsync(
        () -> {
          // Step 1: validate and authorize the action
          validateAndAuthorize(context, tagUrnStrs, targetUrnStr, input);

          // Step 2: raise the proposal.
          try {
            if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
              // Propose Schema Field Tags
              return this.actionRequestService
                  .proposeSchemaFieldTags(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      input.getSubResource(),
                      toUrns(tagUrnStrs),
                      description)
                  .toString();
            } else {
              // Propose Asset Tags
              return this.actionRequestService
                  .proposeEntityTags(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      toUrns(tagUrnStrs),
                      description)
                  .toString();
            }
          } catch (Exception e) {
            if (e instanceof ActionRequestService.MalformedActionRequestException) {
              throw new DataHubGraphQLException(
                  "Failed to propose tags: " + e.getMessage(), DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyAppliedException) {
              throw new DataHubGraphQLException(
                  "Tags are already applied to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyRequestedException) {
              throw new DataHubGraphQLException(
                  "Tags have already been proposed to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof RemoteInvocationException) {
              throw new DataHubGraphQLException(
                  "Failed to create tag proposal. Encountered an error while attempting to reach the downstream service: "
                      + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            } else if (e instanceof EntityDoesNotExistException) {
              throw new DataHubGraphQLException(
                  "Failed to create tag proposal: " + e.getMessage(),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            } else {
              throw new DataHubGraphQLException(
                  "Failed to propose tags: " + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR,
                  e);
            }
          }
        });
  }

  private void validateAndAuthorize(
      QueryContext context, List<String> tagUrnStrs, String targetUrnStr, ProposeTagsInput input) {
    try {
      toUrns(tagUrnStrs);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid tag urns provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    try {
      toUrn(targetUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid resource urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!ProposalUtils.isAuthorizedToProposeTags(
        context, toUrn(targetUrnStr), input.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (tagUrnStrs.isEmpty()) {
      throw new DataHubGraphQLException(
          "No tags provided to propose", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!tagUrnStrs.stream()
        .allMatch(
            urnStr -> UrnUtils.getUrn(urnStr).getEntityType().equals(Constants.TAG_ENTITY_NAME))) {
      throw new DataHubGraphQLException(
          "All provided tags must be of type 'tag'", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())
        && input.getSubResource() == null) {
      throw new DataHubGraphQLException(
          "subResource field must be provided for proposing tags on dataset fields",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (input.getSubResource() != null
        && !SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
      throw new DataHubGraphQLException(
          "Unsupported subResourceType type 'none' provided", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }
}
