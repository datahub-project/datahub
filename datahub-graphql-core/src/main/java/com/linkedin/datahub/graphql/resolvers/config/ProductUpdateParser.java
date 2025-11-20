package com.linkedin.datahub.graphql.resolvers.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility for parsing product update JSON into GraphQL ProductUpdate objects.
 *
 * <p>Handles validation, field extraction, and error cases for product update data.
 */
@Slf4j
public class ProductUpdateParser {

  private ProductUpdateParser() {
    // Utility class, no instantiation
  }

  /**
   * Parse JSON into a ProductUpdate object without clientId decoration.
   *
   * @param jsonOpt Optional JSON node containing product update data
   * @return ProductUpdate object if parsing succeeds and update is enabled, null otherwise
   */
  @Nullable
  public static ProductUpdate parseProductUpdate(@Nonnull Optional<JsonNode> jsonOpt) {
    return parseProductUpdate(jsonOpt, null);
  }

  /**
   * Parse JSON into a ProductUpdate object, decorating the ctaLink with clientId if provided.
   *
   * @param jsonOpt Optional JSON node containing product update data
   * @param clientId Optional client ID to append to ctaLink as a query parameter
   * @return ProductUpdate object if parsing succeeds and update is enabled, null otherwise
   */
  @Nullable
  public static ProductUpdate parseProductUpdate(
      @Nonnull Optional<JsonNode> jsonOpt, @Nullable String clientId) {
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

    // Decorate ctaLink with clientId if provided
    if (clientId != null && !clientId.trim().isEmpty() && !ctaLink.isEmpty()) {
      ctaLink = decorateUrlWithClientId(ctaLink, clientId);
    }

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

    return productUpdate;
  }

  /**
   * Decorates a URL with a clientId query parameter.
   *
   * <p>Adds "?q={clientId}" if the URL has no query parameters, or "&q={clientId}" if it already
   * has query parameters.
   *
   * @param url The URL to decorate
   * @param clientId The client ID to append
   * @return The decorated URL
   */
  @Nonnull
  private static String decorateUrlWithClientId(@Nonnull String url, @Nonnull String clientId) {
    try {
      String encodedClientId = URLEncoder.encode(clientId, StandardCharsets.UTF_8.toString());
      String separator = url.contains("?") ? "&" : "?";
      return url + separator + "q=" + encodedClientId;
    } catch (UnsupportedEncodingException e) {
      log.warn("Failed to URL-encode clientId, using original URL: {}", e.getMessage());
      return url;
    }
  }
}
