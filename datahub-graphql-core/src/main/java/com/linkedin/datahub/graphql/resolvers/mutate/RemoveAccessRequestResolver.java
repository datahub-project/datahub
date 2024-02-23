package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.RemoveAccessRequestInput;
import com.linkedin.datahub.graphql.resolvers.mutate.util.AccessRequestUtils;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveAccessRequestResolver implements DataFetcher<CompletableFuture<Boolean>> {

    private final EntityService _entityService;

    @Override
    public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
        final RemoveAccessRequestInput input =
                bindArgument(environment.getArgument("input"), RemoveAccessRequestInput.class);

        Urn resourceUrn = Urn.createFromString(input.getResourceUrn());
        Urn requesterUrn = Urn.createFromString(input.getRequesterUrn());
        Long requestedAt = input.getRequestedAt();

        if (!AccessRequestUtils.isAuthorizedToRemoveAccessRequest(
                environment.getContext(), resourceUrn, requesterUrn, requestedAt, _entityService)) {
            throw new AuthorizationException(
                    "Unauthorized to perform this action. Please contact your DataHub administrator.");
        }

        return CompletableFuture.supplyAsync(
                () -> {
                    AccessRequestUtils.validateRemoveInput(resourceUrn, requesterUrn, requestedAt, _entityService);
                    try {
                        log.debug("Removing Access Request: {}", input);

                        Urn actor =
                                CorpuserUrn.createFromString(
                                        ((QueryContext) environment.getContext()).getActorUrn());
                        AccessRequestUtils.removeAccessRequest(
                                resourceUrn, requesterUrn, requestedAt, actor, _entityService);
                        return true;
                    } catch (Exception e) {
                        log.error(
                                "Failed to remove access request from resource with input {}, {}",
                                input,
                                e.getMessage());
                        throw new RuntimeException(
                                String.format(
                                        "Failed to remove access request from resource with input  %s", input),
                                e);
                    }
                });
    }
}
