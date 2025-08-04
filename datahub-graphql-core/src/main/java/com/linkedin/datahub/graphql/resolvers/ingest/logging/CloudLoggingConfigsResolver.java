package com.linkedin.datahub.graphql.resolvers.ingest.logging;

import com.linkedin.datahub.graphql.generated.CloudLoggingConfigsResolverResult;
import com.linkedin.metadata.config.ExecutorConfiguration;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
/** Resolver for cloud logging configurations. */
public class CloudLoggingConfigsResolver
    implements DataFetcher<CompletableFuture<CloudLoggingConfigsResolverResult>> {

  private final ExecutorConfiguration _executorConfiguration;

  public CloudLoggingConfigsResolver(@Nonnull final ExecutorConfiguration executorConfiguration) {
    _executorConfiguration =
        Objects.requireNonNull(executorConfiguration, "executorConfiguration must not be null");
  }

  @Override
  public CompletableFuture<CloudLoggingConfigsResolverResult> get(
      final DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.completedFuture(
        new CloudLoggingConfigsResolverResult(
            _executorConfiguration.getCloudLoggingS3Bucket(),
            _executorConfiguration.getCloudLoggingS3Prefix(),
            _executorConfiguration.isRemoteExecutorLoggingEnabled()));
  }
}
