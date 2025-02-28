package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrn;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.OwnerInput;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.generated.ProposeOwnersInput;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.EntityDoesNotExistException;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver for proposing one or more owners. */
@Slf4j
@RequiredArgsConstructor
public class ProposeOwnersResolver implements DataFetcher<CompletableFuture<String>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Bind Inputs
    final ProposeOwnersInput input =
        bindArgument(environment.getArgument("input"), ProposeOwnersInput.class);
    final List<OwnerInput> owners = input.getOwners();
    final String targetUrnStr = input.getResourceUrn();

    return CompletableFuture.supplyAsync(
        () -> {
          // Step 1: validate and authorize the action
          validateAndAuthorize(context, owners, targetUrnStr);

          // Step 2: raise the proposal.
          try {
            // Propose Asset Owners
            return this.actionRequestService
                .proposeEntityOwners(
                    context.getOperationContext(), toUrn(targetUrnStr), toOwners(owners))
                .toString();
          } catch (Exception e) {
            if (e instanceof ActionRequestService.MalformedActionRequestException) {
              throw new DataHubGraphQLException(
                  "Failed to propose owners: " + e.getMessage(),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyAppliedException) {
              throw new DataHubGraphQLException(
                  "Owners are already applied to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyRequestedException) {
              throw new DataHubGraphQLException(
                  "Owners have already been proposed to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof RemoteInvocationException) {
              throw new DataHubGraphQLException(
                  "Failed to create owners proposal. Encountered an error while attempting to reach the downstream service: "
                      + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            } else if (e instanceof EntityDoesNotExistException) {
              throw new DataHubGraphQLException(
                  "Failed to create owners proposal: " + e.getMessage(),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            } else {
              throw new DataHubGraphQLException(
                  "Failed to propose owners: " + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR,
                  e);
            }
          }
        });
  }

  private void validateAndAuthorize(
      QueryContext context, List<OwnerInput> owners, String targetUrnStr) {
    for (OwnerInput owner : owners) {
      try {
        toUrn(owner.getOwnerUrn());
      } catch (RuntimeException e) {
        throw new DataHubGraphQLException(
            String.format("Invalid owner urn %s provided.", owner.getOwnerUrn()),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
      if (!toUrn(owner.getOwnerUrn()).getEntityType().equals(Constants.CORP_USER_ENTITY_NAME)
          && !toUrn(owner.getOwnerUrn()).getEntityType().equals(Constants.CORP_GROUP_ENTITY_NAME)) {
        throw new DataHubGraphQLException(
            "All provided owners must be of type 'corpuser' or 'corpGroup'",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
      if (owner.getOwnershipTypeUrn() != null) {
        try {
          toUrn(owner.getOwnershipTypeUrn());
        } catch (RuntimeException e) {
          throw new DataHubGraphQLException(
              String.format("Invalid ownership type urn %s provided.", owner.getOwnershipTypeUrn()),
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
        if (!toUrn(owner.getOwnershipTypeUrn())
            .getEntityType()
            .equals(Constants.OWNERSHIP_TYPE_ENTITY_NAME)) {
          throw new DataHubGraphQLException(
              "All provided ownership types must be of type 'ownershipType'",
              DataHubGraphQLErrorCode.BAD_REQUEST);
        }
      }
      if (OwnershipType.CUSTOM.equals(owner.getType()) && owner.getOwnershipTypeUrn() == null) {
        throw new DataHubGraphQLException(
            "Ownership type urn must be provided for custom owner type.",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
    }
    try {
      toUrn(targetUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid resource urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!ProposalUtils.isAuthorizedToProposeOwners(context, toUrn(targetUrnStr))) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (owners.isEmpty()) {
      throw new DataHubGraphQLException(
          "At least one owner must be provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }

  private List<Owner> toOwners(@Nonnull final List<OwnerInput> ownersInput) {
    return ownersInput.stream()
        .map(
            ownerInput -> {
              final Owner owner = new Owner();
              owner.setOwner(toUrn(ownerInput.getOwnerUrn()));
              owner.setSource(new OwnershipSource().setType(OwnershipSourceType.MANUAL));
              if (ownerInput.getType() != null) {
                owner.setType(
                    com.linkedin.common.OwnershipType.valueOf(ownerInput.getType().toString()));
              } else {
                owner.setType(com.linkedin.common.OwnershipType.CUSTOM);
              }
              if (ownerInput.getOwnershipTypeUrn() != null) {
                owner.setTypeUrn(toUrn(ownerInput.getOwnershipTypeUrn()));
              }
              return owner;
            })
        .collect(Collectors.toList());
  }
}
