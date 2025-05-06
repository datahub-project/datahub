package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.resolvers.ingest.CachingEntityIngestionSourceFetcher;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.AssertionMonitorsConfiguration;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for fetching the ingestion source responsible for ingesting a particular
 * entity. This is currently used to determine which source will execute an assertion.
 *
 * <p>Ideally, in the future we leverage a pipeline_name which is placed in the system metadata to
 * do this. For now, we rely on fetching the "last run id" for an entity and backtracing to the
 * ingestion source which produced it.
 */
@Slf4j
public class IngestionSourceForEntityResolver
    implements DataFetcher<CompletableFuture<IngestionSource>> {
  private final CachingEntityIngestionSourceFetcher _ingestionSourceFetcher;

  public IngestionSourceForEntityResolver(
      @Nonnull final EntityClient entityClient,
      @Nonnull final AssertionMonitorsConfiguration assertionMonitorsConfiguration) {
    _ingestionSourceFetcher =
        new CachingEntityIngestionSourceFetcher(entityClient, assertionMonitorsConfiguration);
  }

  @Override
  public CompletableFuture<IngestionSource> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    String entityUrnString = environment.getArgument("urn");
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    final Urn entityUrn = Urn.createFromString(entityUrnString);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return _ingestionSourceFetcher.getIngestionSourceForEntity(entityUrn, context);
          } catch (Exception e) {
            log.error(
                String.format(
                    "Failed to check whether ingestion source for entity %s exists", entityUrn),
                e);
            return null;
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
