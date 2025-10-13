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
 * Resolver for refreshing the product update cache.
 *
 * <p>Clears the cache and fetches fresh data from the remote source, falling back to classpath
 * resource if unavailable.
 */
@Slf4j
public class RefreshProductUpdateResolver implements DataFetcher<CompletableFuture<ProductUpdate>> {

  private final ProductUpdateService _productUpdateService;
  private final FeatureFlags _featureFlags;

  public RefreshProductUpdateResolver(
      @Nonnull final ProductUpdateService productUpdateService,
      @Nonnull final FeatureFlags featureFlags) {
    this._productUpdateService = productUpdateService;
    this._featureFlags = featureFlags;
  }

  @Override
  public CompletableFuture<ProductUpdate> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!_featureFlags.isShowProductUpdates()) {
              log.debug("Product updates feature is disabled");
              return null;
            }

            log.info("Clearing product update cache and refetching");
            _productUpdateService.clearCache();

            ProductUpdate productUpdate =
                ProductUpdateParser.parseProductUpdate(
                    _productUpdateService.getLatestProductUpdate());

            if (productUpdate != null) {
              log.info("Successfully refreshed product update: {}", productUpdate.getId());
            }

            return productUpdate;

          } catch (Exception e) {
            log.warn("Failed to refresh product update: {}", e.getMessage());
            log.debug("Product update refresh error details", e);
            return null;
          }
        });
  }
}
