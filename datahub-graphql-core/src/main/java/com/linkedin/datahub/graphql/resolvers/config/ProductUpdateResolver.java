package com.linkedin.datahub.graphql.resolvers.config;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ProductUpdateService;
import com.linkedin.telemetry.TelemetryClientId;
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
 *
 * <p>Decorates the CTA link with the instance's client ID.
 */
@Slf4j
public class ProductUpdateResolver implements DataFetcher<CompletableFuture<ProductUpdate>> {

  private final ProductUpdateService _productUpdateService;
  private final FeatureFlags _featureFlags;
  private final EntityService<?> _entityService;

  public ProductUpdateResolver(
      @Nonnull final ProductUpdateService productUpdateService,
      @Nonnull final FeatureFlags featureFlags,
      @Nonnull final EntityService<?> entityService) {
    this._productUpdateService = productUpdateService;
    this._featureFlags = featureFlags;
    this._entityService = entityService;
  }

  @Override
  public CompletableFuture<ProductUpdate> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
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

            String clientId = null;
            try {
              clientId = getClientId(context);
              if (clientId != null) {
                log.debug("Retrieved client ID for product update decoration: {}", clientId);
              }
            } catch (Exception e) {
              log.warn(
                  "Failed to retrieve client ID, product update link will not be decorated: {}",
                  e.getMessage());
              log.debug("Client ID retrieval error details", e);
            }

            ProductUpdate productUpdate =
                ProductUpdateParser.parseProductUpdate(
                    _productUpdateService.getLatestProductUpdate(), clientId);

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

  private String getClientId(@Nonnull final QueryContext context) {
    try {
      RecordTemplate clientIdAspect =
          _entityService.getLatestAspect(
              context.getOperationContext(),
              UrnUtils.getUrn(Constants.CLIENT_ID_URN),
              Constants.CLIENT_ID_ASPECT);

      if (clientIdAspect instanceof TelemetryClientId) {
        return ((TelemetryClientId) clientIdAspect).getClientId();
      }
    } catch (Exception e) {
      log.debug("Error retrieving client ID: {}", e.getMessage());
    }
    return null;
  }
}
