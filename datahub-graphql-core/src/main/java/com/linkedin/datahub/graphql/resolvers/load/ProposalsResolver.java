package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.common.urn.Urn;


import com.linkedin.datahub.graphql.QueryContext;
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

import static com.linkedin.metadata.Constants.*;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single {@link LoadableType}.
 *
 *  Note that this resolver expects that {@link DataLoader}s were registered
 *  for the provided {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 */
@AllArgsConstructor
public class ProposalsResolver implements DataFetcher<CompletableFuture<List<ActionRequest>>> {

    private final Function<DataFetchingEnvironment, String> _urnProvider;
    private final EntityClient _entityClient;

    @Override
    public CompletableFuture<List<ActionRequest>> get(DataFetchingEnvironment environment) {

        final QueryContext context = environment.getContext();
        final ActionRequestStatus status = ActionRequestStatus.valueOf(
            environment.getArgumentOrDefault("status", ActionRequestStatus.PENDING.toString())
        );
        final ActionRequestType type = ActionRequestType.valueOf(environment.getArgumentOrDefault("type", null));
        final String urn = _urnProvider.apply(environment);

        Filter filter = ProposalUtils.createActionRequestFilter(type, status, urn, null);

        return CompletableFuture.supplyAsync(() -> {
            try {
                final SearchResult searchResult = _entityClient.filter(
                    ACTION_REQUEST_ENTITY_NAME,
                    filter,
                    null,
                    0,
                    20,
                    context.getAuthentication());

                final Map<Urn, Entity> entities = _entityClient.batchGet(new HashSet<>(searchResult.getEntities()
                    .stream().map(result -> result.getEntity()).collect(Collectors.toList())), context.getAuthentication());
                return ActionRequestUtils.mapActionRequests(entities.values());
            } catch (Exception e) {
                throw new RuntimeException("Failed to load action requests", e);
            }
        });
    }

}
