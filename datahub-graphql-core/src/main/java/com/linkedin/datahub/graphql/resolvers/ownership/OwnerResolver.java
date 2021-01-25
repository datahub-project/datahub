package com.linkedin.datahub.graphql.resolvers.ownership;

import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.loaders.CorpUserLoader;
import com.linkedin.datahub.graphql.mappers.CorpUserMapper;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import java.util.concurrent.CompletableFuture;


/**
 * Resolver responsible for resolving the 'owner' field of the Owner type.
 */
public class OwnerResolver implements DataFetcher<CompletableFuture<CorpUser>> {
    @Override
    public CompletableFuture<CorpUser> get(DataFetchingEnvironment environment) {
        final Owner parent = environment.getSource();
        final DataLoader<String, com.linkedin.identity.CorpUser> dataLoader = environment.getDataLoader(CorpUserLoader.NAME);
        return dataLoader.load(parent.getOwner().getUrn())
                .thenApply(corpUser -> corpUser != null ? CorpUserMapper.map(corpUser) : null);
    }
}
