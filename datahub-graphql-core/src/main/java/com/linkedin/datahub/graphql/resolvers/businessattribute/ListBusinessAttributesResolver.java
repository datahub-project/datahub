package com.linkedin.datahub.graphql.resolvers.businessattribute;

import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class ListBusinessAttributesResolver implements DataFetcher<CompletableFuture<SearchResults>> {
    private final EntityClient _entityClient;
    private static final int DEFAULT_START = 0;
    private static final int DEFAULT_COUNT = 10;

    @Override
    public CompletableFuture<SearchResults> get(DataFetchingEnvironment environment) throws Exception {
        return null;
    }
}
