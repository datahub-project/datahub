package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ListSignalRequestsInput;
import com.linkedin.datahub.graphql.generated.ListSignalRequestsResult;
import com.linkedin.datahub.graphql.generated.SignalRequest;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Lists all Signal Requests given a list of Execution Request urns. */
public class ListSignalRequestsResolver
    implements DataFetcher<CompletableFuture<ListSignalRequestsResult>> {
  private final EntityClient _entityClient;

  public ListSignalRequestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListSignalRequestsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    final ListSignalRequestsInput input =
        bindArgument(environment.getArgument("input"), ListSignalRequestsInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final Set<Urn> urns =
                input.getUrns().stream()
                    .map(
                        (urnStr) -> {
                          try {
                            return Urn.createFromString(urnStr);
                          } catch (Exception e) {
                            throw new RuntimeException("Failed to convert urn", e);
                          }
                        })
                    .collect(Collectors.toSet());

            Map<Urn, EntityResponse> aspects =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.EXECUTION_REQUEST_ENTITY_NAME,
                    urns,
                    ImmutableSet.of(Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME));

            List<SignalRequest> signalRequests = new ArrayList<>();
            for (Map.Entry<Urn, EntityResponse> entry : aspects.entrySet()) {
              Urn urn = entry.getKey();
              EntityResponse response = entry.getValue();
              final SignalRequest signalRequest =
                  SignalRequestUtils.mapSignalRequest(urn, response);
              if (signalRequest != null) {
                signalRequests.add(signalRequest);
              }
            }

            // Now that we have entities we can bind this to a result.
            final ListSignalRequestsResult result = new ListSignalRequestsResult();
            result.setTotal(signalRequests.size());
            result.setSignalRequests(signalRequests);
            return result;

          } catch (Exception e) {
            throw new RuntimeException("Failed to list ingestion sources", e);
          }
        });
  }
}
