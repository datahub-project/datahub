package com.linkedin.datahub.graphql;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.client.EntityClient;
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

    EntityClient _entityClient;
    String _entityType;
    String _aspectName;

    @Override
    public CompletableFuture<SubTypes> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();
            Optional<SubTypes> subType;
            try {
                subType = _entityClient.getVersionedAspect(urn, _aspectName, 0L, SubTypes.class,  context.getAuthentication());
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
