package com.linkedin.datahub.graphql;

import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@AllArgsConstructor
public class SubTypesResolver implements DataFetcher<CompletableFuture<SubTypes>> {

    EntityClient _entityClient;
    String _entityType;
    String _aspectName;

    @Override
    @Nullable
    public CompletableFuture<SubTypes> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            final QueryContext context = environment.getContext();
            SubTypes subType = null;
            final String urnStr = ((Entity) environment.getSource()).getUrn();
            try {
                final Urn urn = Urn.createFromString(urnStr);
                EntityResponse entityResponse = _entityClient.batchGetV2(urn.getEntityType(), Collections.singleton(urn),
                    Collections.singleton(_aspectName), context.getAuthentication()).get(urn);
                if (entityResponse != null && entityResponse.getAspects().containsKey(_aspectName)) {
                    subType = new SubTypes(entityResponse.getAspects().get(_aspectName).getValue().data());
                }
            } catch (RemoteInvocationException | URISyntaxException e) {
                throw new RuntimeException("Failed to fetch aspect " + _aspectName + " for urn " + urnStr + " ", e);
            }
            return subType;
        });
    }
}
