package com.linkedin.datahub.graphql.resolvers.config;

import com.linkedin.datahub.graphql.QueryContext;
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
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            if (!_featureFlags.isShowProductUpdates()) {
              log.debug("Product updates feature is disabled");
              return null;
            }

            ProductUpdate productUpdate =
                ProductUpdateParser.parseProductUpdate(
                    _productUpdateService.getLatestProductUpdate());

            if (productUpdate != null) {
              log.debug("Successfully resolved product update: {}", productUpdate.getId());
            }

            return productUpdate;

          } catch (Exception e) {
            log.warn("Failed to resolve product update: {}", e.getMessage());
            log.debug("Product update resolution error details", e);
            return null;
          }
        });
  }
}
