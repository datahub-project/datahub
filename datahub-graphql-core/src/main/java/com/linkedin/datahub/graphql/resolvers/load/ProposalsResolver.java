package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils.getGroupAndRoleUrns;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ActionRequest;
import com.linkedin.datahub.graphql.generated.ActionRequestStatus;
import com.linkedin.datahub.graphql.generated.ActionRequestType;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.dataloader.DataLoader;

/**
 * Generic GraphQL resolver responsible for
 *
 * <p>1. Retrieving a single input urn. 2. Resolving a single {@link LoadableType}.
 *
 * <p>Note that this resolver expects that {@link DataLoader}s were registered for the provided
 * {@link LoadableType} under the name provided by {@link LoadableType#name()}
 */
@AllArgsConstructor
public class ProposalsResolver implements DataFetcher<CompletableFuture<List<ActionRequest>>> {
  // WARNING: If a user has more than 1000 proposals active on an asset, this will need to be
  // increased.
  private final int MAX_RELATED_PROPOSALS_TO_FETCH = 1000;

  private final Function<DataFetchingEnvironment, String> _urnProvider;
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<List<ActionRequest>> get(DataFetchingEnvironment environment) {

    final QueryContext context = environment.getContext();
    final ActionRequestStatus status =
        ActionRequestStatus.valueOf(
            environment.getArgumentOrDefault("status", ActionRequestStatus.PENDING.toString()));

    String typeVal = environment.getArgumentOrDefault("type", null);
    final ActionRequestType type = (typeVal != null) ? ActionRequestType.valueOf(typeVal) : null;

    final String urn = _urnProvider.apply(environment);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            Urn actorUrn = Urn.createFromString(context.getActorUrn());
            ActionRequestUtils.AssignedUrns groupAndRoleUrns =
                getGroupAndRoleUrns(context.getOperationContext(), actorUrn, _entityClient);
            List<Urn> groupUrns = groupAndRoleUrns.getGroupUrns();
            List<Urn> roleUrns = groupAndRoleUrns.getRoleUrns();

            Filter filter =
                ProposalUtils.createFilter(
                    actorUrn,
                    groupUrns,
                    roleUrns,
                    type,
                    status,
                    Urn.createFromString(urn),
                    null,
                    null);
            final SearchResult searchResult =
                _entityClient.filter(
                    context.getOperationContext(),
                    ACTION_REQUEST_ENTITY_NAME,
                    filter,
                    null,
                    0,
                    MAX_RELATED_PROPOSALS_TO_FETCH);

            final Map<Urn, Entity> entities =
                _entityClient.batchGet(
                    context.getOperationContext(),
                    new HashSet<>(
                        searchResult.getEntities().stream()
                            .map(result -> result.getEntity())
                            .collect(Collectors.toList())));
            return ActionRequestUtils.mapActionRequests(context, entities.values());
          } catch (Exception e) {
            throw new RuntimeException("Failed to load action requests", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
