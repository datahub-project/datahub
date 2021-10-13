package com.linkedin.datahub.graphql;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.client.AspectClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
public class SubTypesResolver implements DataFetcher<CompletableFuture<SubTypes>> {

    AspectClient _aspectClient;
    String _entityType;
    String _aspectName;

    @Override
    public CompletableFuture<SubTypes> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();
            Optional<SubTypes> subType;
            try {
                subType = _aspectClient.getVersionedAspect(urn, _aspectName, 0L, context.getActor(), SubTypes.class);
            } catch (RemoteInvocationException e) {
                throw new RuntimeException("Failed to fetch aspect " + _aspectName + " for urn " + urn + " ", e);
            }
            if (subType.isPresent()) {
                return subType.get();
            } else {
                return null;
            }
        });
    }
}
