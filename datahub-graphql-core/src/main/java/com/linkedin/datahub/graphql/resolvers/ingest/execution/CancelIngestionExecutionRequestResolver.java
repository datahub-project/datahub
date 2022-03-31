package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.CancelIngestionExecutionRequestInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestSignal;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Cancels a requested ingestion execution by emitting a KILL signal.
 */
public class CancelIngestionExecutionRequestResolver implements DataFetcher<CompletableFuture<String>> {

  private static final String KILL_EXECUTION_REQUEST_SIGNAL = "KILL";

  private final EntityClient _entityClient;

  public CancelIngestionExecutionRequestResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (IngestionAuthUtils.canManageIngestion(context)) {

        final CancelIngestionExecutionRequestInput input =
            bindArgument(environment.getArgument("input"), CancelIngestionExecutionRequestInput.class);

        try {

          final MetadataChangeProposal proposal = new MetadataChangeProposal();
          proposal.setEntityUrn(Urn.createFromString(input.getExecutionRequestUrn()));

          final Urn ingestionSourceUrn = Urn.createFromString(input.getIngestionSourceUrn());
          final Map<Urn, EntityResponse> response =
              _entityClient.batchGetV2(Constants.INGESTION_SOURCE_ENTITY_NAME, ImmutableSet.of(ingestionSourceUrn),
                  ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME), context.getAuthentication());

          if (!response.containsKey(ingestionSourceUrn)) {
            throw new DataHubGraphQLException(
                String.format("Failed to find ingestion source with urn %s", ingestionSourceUrn.toString()),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }

          final EnvelopedAspect envelopedInfo =
              response.get(ingestionSourceUrn).getAspects().get(Constants.INGESTION_INFO_ASPECT_NAME);
          final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

          // Build the arguments map.
          final ExecutionRequestSignal execSignal = new ExecutionRequestSignal();
          execSignal.setSignal(KILL_EXECUTION_REQUEST_SIGNAL); // Requests a kill of the running task.
          execSignal.setExecutorId(ingestionSourceInfo.getConfig().getExecutorId(), SetMode.IGNORE_NULL);
          execSignal.setCreatedAt(new AuditStamp()
              .setTime(System.currentTimeMillis())
              .setActor(Urn.createFromString(context.getActorUrn()))
          );
          proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
          proposal.setAspectName(Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME);
          proposal.setAspect(GenericRecordUtils.serializeAspect(execSignal));
          proposal.setChangeType(ChangeType.UPSERT);

          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to submit cancel signal %s", input.toString()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}