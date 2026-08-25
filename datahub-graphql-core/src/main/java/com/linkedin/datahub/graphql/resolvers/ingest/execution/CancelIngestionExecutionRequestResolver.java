package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CancelIngestionExecutionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSignal;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Cancels a requested ingestion execution by emitting a KILL signal. */
public class CancelIngestionExecutionRequestResolver
    implements DataFetcher<CompletableFuture<String>> {

  private static final String KILL_EXECUTION_REQUEST_SIGNAL = "KILL";

  private final EntityClient _entityClient;

  public CancelIngestionExecutionRequestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final CancelIngestionExecutionRequestInput input =
              bindArgument(
                  environment.getArgument("input"), CancelIngestionExecutionRequestInput.class);

          try {
            final Urn claimedIngestionSourceUrn =
                Urn.createFromString(input.getIngestionSourceUrn());
            final Urn executionRequestUrn = UrnUtils.getUrn(input.getExecutionRequestUrn());

            // Bind cancel to the execution request's actual source so EXECUTE on source A cannot
            // kill a run belonging to source B.
            final Map<Urn, EntityResponse> executionRequestResponse =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    EXECUTION_REQUEST_ENTITY_NAME,
                    ImmutableSet.of(executionRequestUrn),
                    ImmutableSet.of(EXECUTION_REQUEST_INPUT_ASPECT_NAME));

            if (!executionRequestResponse.containsKey(executionRequestUrn)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to find execution request with urn %s", executionRequestUrn),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }

            final EnvelopedAspect envelopedInput =
                executionRequestResponse
                    .get(executionRequestUrn)
                    .getAspects()
                    .get(EXECUTION_REQUEST_INPUT_ASPECT_NAME);
            if (envelopedInput == null) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to find execution request input for urn %s", executionRequestUrn),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }

            final ExecutionRequestInput executionRequestInput =
                new ExecutionRequestInput(envelopedInput.getValue().data());
            final Urn actualIngestionSourceUrn =
                executionRequestInput.getSource() != null
                    ? executionRequestInput.getSource().getIngestionSource()
                    : null;
            if (actualIngestionSourceUrn == null) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Execution request %s is not associated with an ingestion source",
                      executionRequestUrn),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }
            if (!Objects.equals(actualIngestionSourceUrn, claimedIngestionSourceUrn)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Execution request %s does not belong to ingestion source %s",
                      executionRequestUrn, claimedIngestionSourceUrn),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }

            if (!IngestionAuthUtils.canExecuteIngestion(context, actualIngestionSourceUrn)) {
              throw new AuthorizationException(
                  "Unauthorized to perform this action. Please contact your DataHub administrator.");
            }

            final Map<Urn, EntityResponse> response =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    INGESTION_SOURCE_ENTITY_NAME,
                    ImmutableSet.of(actualIngestionSourceUrn),
                    ImmutableSet.of(INGESTION_INFO_ASPECT_NAME));

            if (!response.containsKey(actualIngestionSourceUrn)) {
              throw new DataHubGraphQLException(
                  String.format(
                      "Failed to find ingestion source with urn %s", actualIngestionSourceUrn),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            }

            final EnvelopedAspect envelopedInfo =
                response.get(actualIngestionSourceUrn).getAspects().get(INGESTION_INFO_ASPECT_NAME);
            final DataHubIngestionSourceInfo ingestionSourceInfo =
                new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

            // Build the arguments map.
            final ExecutionRequestSignal execSignal = new ExecutionRequestSignal();
            execSignal.setSignal(
                KILL_EXECUTION_REQUEST_SIGNAL); // Requests a kill of the running task.
            execSignal.setExecutorId(
                ingestionSourceInfo.getConfig().getExecutorId(), SetMode.IGNORE_NULL);
            execSignal.setCreatedAt(
                new AuditStamp()
                    .setTime(System.currentTimeMillis())
                    .setActor(Urn.createFromString(context.getActorUrn())));
            final MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    executionRequestUrn, EXECUTION_REQUEST_SIGNAL_ASPECT_NAME, execSignal);
            return _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
          } catch (AuthorizationException e) {
            throw e;
          } catch (DataHubGraphQLException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to submit cancel signal %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
