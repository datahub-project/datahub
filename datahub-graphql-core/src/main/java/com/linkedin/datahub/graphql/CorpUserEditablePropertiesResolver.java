package com.linkedin.datahub.graphql;

import com.linkedin.common.SubTypes;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@AllArgsConstructor
public class CorpUserEditablePropertiesResolver implements DataFetcher<CompletableFuture<CorpUserEditableInfo>> {

    EntityClient _entityClient;
    String _entityType;
    String _aspectName;

    @Override
    public CompletableFuture<CorpUserEditableInfo> get(DataFetchingEnvironment environment) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            final QueryContext context = environment.getContext();
            final String urn = ((Entity) environment.getSource()).getUrn();
            Optional<CorpUserEditableInfo> corpUserEditableProperties;
            try {
                corpUserEditableProperties = _entityClient.getVersionedAspect(urn, _aspectName, 0L, CorpUserEditableInfo.class,  context.getAuthentication());
            } catch (RemoteInvocationException e) {
                throw new RuntimeException("Failed to fetch aspect " + _aspectName + " for urn " + urn + " ", e);
            }
            if (corpUserEditableProperties.isPresent()) {
                return corpUserEditableProperties.get();
            } else {
                return null;
            }
        });
    }
}
