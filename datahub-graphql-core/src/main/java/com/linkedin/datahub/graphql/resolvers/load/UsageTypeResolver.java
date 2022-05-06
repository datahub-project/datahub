package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.UsageStatsKey;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.pegasus2avro.usage.UsageQueryResult;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import lombok.extern.slf4j.Slf4j;


/**
 * Generic GraphQL resolver responsible for
 *
 *    1. Retrieving a single input urn.
 *    2. Resolving a single {@link LoadableType}.
 *
 *  Note that this resolver expects that {@link DataLoader}s were registered
 *  for the provided {@link LoadableType} under the name provided by {@link LoadableType#name()}
 *
 */
@Slf4j
public class UsageTypeResolver implements DataFetcher<CompletableFuture<UsageQueryResult>> {

    @Override
    public CompletableFuture<UsageQueryResult> get(DataFetchingEnvironment environment) {
        final DataLoader<UsageStatsKey, UsageQueryResult> loader = environment.getDataLoaderRegistry().getDataLoader("UsageQueryResult");

        String deprecatedResource = environment.getArgument("resource");
        if (deprecatedResource != null) {
            log.info("You no longer need to provide the deprecated `resource` param to usageStats"
                + "resolver. Provided: {}", deprecatedResource);
        }
        final String resource = ((Entity) environment.getSource()).getUrn();
        UsageTimeRange duration = UsageTimeRange.valueOf(environment.getArgument("range"));

        UsageStatsKey key = new UsageStatsKey(resource, duration);

        return loader.load(key);
    }
}
