package com.linkedin.datahub.graphql.resolvers.query;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.loaders.DatasetLoader;
import com.linkedin.datahub.graphql.mappers.DatasetMapper;
import com.linkedin.datahub.graphql.resolvers.AuthorizedResolver;

import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.Constants.*;


/**
 * Resolver responsible for resolving the 'dataset' field of the Query type
 */
public class DatasetResolver extends AuthorizedResolver<CompletableFuture<Dataset>> {
    @Override
    protected boolean isAuthorized(DataFetchingEnvironment environment) {
        // Always authorize reads, for now. Override this resolver to customize authorization.
        return true;
    }

    @Override
    public CompletableFuture<Dataset> authorizedGet(DataFetchingEnvironment environment) {
        final DataLoader<String, com.linkedin.dataset.Dataset> dataLoader = environment.getDataLoader(DatasetLoader.NAME);
        return dataLoader.load(environment.getArgument(URN_FIELD_NAME))
                .thenApply(dataset -> dataset != null ? DatasetMapper.map(dataset) : null);
    }
}
