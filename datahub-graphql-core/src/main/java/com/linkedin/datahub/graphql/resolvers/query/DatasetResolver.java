package com.linkedin.datahub.graphql.resolvers.query;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.loaders.DatasetLoader;
import com.linkedin.datahub.graphql.mappers.DatasetMapper;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.Constants.*;


/**
 * Resolver responsible for resolving the 'dataset' field of the Query type
 */
public class DatasetResolver implements DataFetcher<CompletableFuture<Dataset>> {
    @Override
    public CompletableFuture<Dataset> get(DataFetchingEnvironment environment) {
        final DataLoader<String, com.linkedin.dataset.Dataset> dataLoader = environment.getDataLoader(DatasetLoader.NAME);
        return dataLoader.load(environment.getArgument(URN_FIELD_NAME))
                .thenApply(dataset -> dataset != null ? DatasetMapper.map(dataset) : null);
    }
}
