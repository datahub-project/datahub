package com.linkedin.datahub.graphql.resolvers.corpuser;

import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.loaders.CorpUserLoader;
import com.linkedin.datahub.graphql.mappers.CorpUserMapper;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver responsible for resolving the 'manager' field of the CorpUser type.
 */
public class ManagerResolver implements DataFetcher<CompletableFuture<CorpUser>> {
    @Override
    public CompletableFuture<CorpUser> get(DataFetchingEnvironment environment) throws Exception {
        final CorpUserInfo parent = environment.getSource();
        final DataLoader<String, com.linkedin.identity.CorpUser> dataLoader = environment.getDataLoader(CorpUserLoader.NAME);
        return dataLoader.load(parent.getManager().getUrn())
                .thenApply(corpUser -> corpUser != null ? CorpUserMapper.map(corpUser) : null);
    }
}
