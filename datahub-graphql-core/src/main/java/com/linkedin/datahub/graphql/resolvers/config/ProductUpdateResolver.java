package com.linkedin.datahub.graphql.resolvers.config;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.metadata.service.ProductUpdateService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for fetching the latest product update information.
 *
 * <p>Attempts to fetch from remote URL (with 6-hour cache), falling back to bundled classpath
 * resource if unavailable. Returns null only if both remote and fallback fail, or if the feature is
 * disabled.
 *
 * <p>Supports an optional {@code refreshCache} argument to clear the cache before fetching.
 */
@Slf4j
public class ProductUpdateResolver implements DataFetcher<CompletableFuture<ProductUpdate>> {

  private final ProductUpdateService _productUpdateService;
  private final FeatureFlags _featureFlags;

  public ProductUpdateResolver(
      @Nonnull final ProductUpdateService productUpdateService,
      @Nonnull final FeatureFlags featureFlags) {
    this._productUpdateService = productUpdateService;
    this._featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<ProductUpdate> get(DataFetchingEnvironment environment) {
    final Boolean refreshCache = environment.getArgument("refreshCache");
    final boolean shouldRefresh = refreshCache != null && refreshCache;

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!_featureFlags.isShowProductUpdates()) {
              log.debug("Product updates feature is disabled");
              return null;
            }

            if (shouldRefresh) {
              log.info("Clearing product update cache and refetching");
              _productUpdateService.clearCache();
            }

            ProductUpdate productUpdate =
                ProductUpdateParser.parseProductUpdate(
                    _productUpdateService.getLatestProductUpdate());

            if (productUpdate != null) {
              log.debug(
                  "Successfully {} product update: {}",
                  shouldRefresh ? "refreshed" : "resolved",
                  productUpdate.getId());
            }

            return productUpdate;

          } catch (Exception e) {
            log.warn(
                "Failed to {} product update: {}",
                shouldRefresh ? "refresh" : "resolve",
                e.getMessage());
            log.debug("Product update error details", e);
            return null;
          }
        });
  }
}
