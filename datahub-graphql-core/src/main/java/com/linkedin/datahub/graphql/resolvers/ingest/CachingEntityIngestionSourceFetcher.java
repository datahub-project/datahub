package com.linkedin.datahub.graphql.resolvers.ingest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.client.EntityClient;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Fetches the ingestion source associated with a given entity urn */
@Slf4j
public class CachingEntityIngestionSourceFetcher {
  private final EntityClient _entityClient;
  private final Cache<Urn, IngestionSource> _ingestionSourceCache;

  public CachingEntityIngestionSourceFetcher(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _ingestionSourceCache =
        CacheBuilder.newBuilder().expireAfterWrite(60, TimeUnit.MINUTES).build();
  }

  @Nullable
  public IngestionSource getIngestionSourceForEntity(
      @Nonnull final Urn entityUrn, @Nonnull final QueryContext context) throws Exception {
    final IngestionSource cachedSource = _ingestionSourceCache.getIfPresent(entityUrn);
    if (cachedSource != null) {
      return cachedSource;
    }
    final IngestionSource maybeSource =
        IngestionResolverUtils.getIngestionSourceForEntity(this._entityClient, entityUrn, context);
    if (maybeSource != null) {
      _ingestionSourceCache.put(entityUrn, maybeSource);
    }
    return maybeSource;
  }
}
