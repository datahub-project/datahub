package com.linkedin.datahub.graphql.resolvers.ingest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.AssertionMonitorsConfiguration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Fetches the ingestion source associated with a given entity urn that can execute assertions */
@Slf4j
public class CachingEntityIngestionSourceFetcher {
  private final EntityClient _entityClient;
  private final Cache<Urn, IngestionSource> _ingestionSourceCache;
  private final AssertionMonitorsConfiguration _assertionMonitorsConfig;

  public CachingEntityIngestionSourceFetcher(
      @Nonnull final EntityClient entityClient,
      @Nonnull final AssertionMonitorsConfiguration assertionMonitorsConfig) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _assertionMonitorsConfig = assertionMonitorsConfig;
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
    final Set<String> aspectNames = getAssertionSourceAspectNames();
    final IngestionSource maybeSource =
        IngestionResolverUtils.getIngestionSourceForEntity(
            this._entityClient, context, entityUrn, aspectNames);
    if (maybeSource != null) {
      _ingestionSourceCache.put(entityUrn, maybeSource);
    }
    return maybeSource;
  }

  private Set<String> getAssertionSourceAspectNames() {
    final String csvAspectNames = _assertionMonitorsConfig.getResolveIngestionSourceForAspects();
    if (csvAspectNames == null) {
      return null;
    }
    return new HashSet<>(List.of(csvAspectNames.split(",")));
  }
}
