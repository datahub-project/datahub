package com.linkedin.datahub.graphql.resolvers.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.metadata.service.ProductUpdateService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
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
            // Check if feature is enabled
            if (!_featureFlags.isShowProductUpdates()) {
              log.debug("Product updates feature is disabled");
              return null;
            }

            // Fetch the JSON from the service
            Optional<JsonNode> jsonOpt = _productUpdateService.getLatestProductUpdate();
            if (jsonOpt.isEmpty()) {
              log.debug("No product update JSON available");
              return null;
            }

            JsonNode json = jsonOpt.get();

            // Parse and validate required fields
            if (!json.has("enabled") || !json.has("id") || !json.has("title")) {
              log.warn("Product update JSON missing required fields (enabled, id, or title)");
              return null;
            }

            boolean enabled = json.get("enabled").asBoolean();
            if (!enabled) {
              log.debug("Product update is disabled in JSON");
              return null;
            }

            String id = json.get("id").asText();
            String title = json.get("title").asText();
            String ctaText = json.has("ctaText") ? json.get("ctaText").asText() : "Learn more";
            String ctaLink = json.has("ctaLink") ? json.get("ctaLink").asText() : "";

            // Build the ProductUpdate response
            ProductUpdate productUpdate = new ProductUpdate();
            productUpdate.setEnabled(enabled);
            productUpdate.setId(id);
            productUpdate.setTitle(title);
            productUpdate.setCtaText(ctaText);
            productUpdate.setCtaLink(ctaLink);

            // Optional fields
            if (json.has("description")) {
              productUpdate.setDescription(json.get("description").asText());
            }
            if (json.has("image")) {
              productUpdate.setImage(json.get("image").asText());
            }

            log.debug("Successfully resolved product update: {}", id);
            return productUpdate;

          } catch (Exception e) {
            log.warn("Failed to resolve product update: {}", e.getMessage());
            log.debug("Product update resolution error details", e);
            return null;
          }
        });
  }
}
