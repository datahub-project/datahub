package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.datahub.authorization.AuthUtil.isAuthorized;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.buildMetadataChangeProposalWithUrn;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.StoreExecutionRequestUploadLocationInput;
import com.linkedin.datahub.graphql.generated.StoreExecutionRequestUploadLocationResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestArtifactsLocation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class StoreExecutionRequestUploadLocationResolver
    implements DataFetcher<CompletableFuture<StoreExecutionRequestUploadLocationResult>> {

  private static final String EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME =
      "dataHubExecutionRequestArtifactsLocation";

  private final EntityClient entityClient;

  @Override
  public CompletableFuture<StoreExecutionRequestUploadLocationResult> get(
      final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final StoreExecutionRequestUploadLocationInput input =
              bindArgument(
                  environment.getArgument("input"), StoreExecutionRequestUploadLocationInput.class);

          try {
            final Urn executionRequestUrn = UrnUtils.getUrn(input.getExecutionRequestUrn());

            // Validate that the URN is an execution request
            if (!executionRequestUrn.getEntityType().equals(EXECUTION_REQUEST_ENTITY_NAME)) {
              throw new DataHubGraphQLException(
                  "URN must be an execution request URN", DataHubGraphQLErrorCode.BAD_REQUEST);
            }

            // Check authorization - user should be able to access this execution request
            if (!canStoreUploadLocation(context, executionRequestUrn)) {
              throw new AuthorizationException(
                  "Unauthorized to store upload location for this execution request");
            }

            // Create the ExecutionRequestArtifactsLocation aspect
            final ExecutionRequestArtifactsLocation uploadLocation =
                new ExecutionRequestArtifactsLocation();
            uploadLocation.setLocation(input.getLocation());

            // Build the metadata change proposal
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    executionRequestUrn,
                    EXECUTION_REQUEST_ARTIFACTS_LOCATION_ASPECT_NAME,
                    uploadLocation);

            // Ingest the proposal
            entityClient.ingestProposal(context.getOperationContext(), proposal, false);

            log.info(
                "Successfully stored upload location for execution request: {}, location: {}",
                executionRequestUrn,
                input.getLocation());

            final StoreExecutionRequestUploadLocationResult result =
                new StoreExecutionRequestUploadLocationResult();
            result.setSuccess(true);
            return result;

          } catch (Exception e) {
            log.error("Failed to store upload location for execution request", e);
            throw new RuntimeException(
                String.format("Failed to store upload location: %s", e.getMessage()), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private boolean canStoreUploadLocation(
      final QueryContext context, final Urn executionRequestUrn) {
    return isAuthorized(context.getOperationContext(), PoliciesConfig.MANAGE_INGESTION_PRIVILEGE);
  }
}
