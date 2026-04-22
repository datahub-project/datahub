package com.linkedin.datahub.graphql.resolvers.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.datahub.graphql.generated.ProductUpdateFeature;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

    // Build the ProductUpdate response
    ProductUpdate productUpdate = new ProductUpdate();
    productUpdate.setEnabled(enabled);
    productUpdate.setId(id);
    productUpdate.setTitle(title);

    // Optional fields
    if (json.has("header")) {
      productUpdate.setHeader(json.get("header").asText());
    }
    if (json.has("requiredVersion")) {
      productUpdate.setRequiredVersion(json.get("requiredVersion").asText());
    }
    if (json.has("description")) {
      productUpdate.setDescription(json.get("description").asText());
    }
    if (json.has("image")) {
      productUpdate.setImage(json.get("image").asText());
    }

    // Parse primary CTA (new format) - preferred over legacy ctaText/ctaLink
    if (json.has("primaryCtaText") && json.has("primaryCtaLink")) {
      String primaryCtaText = json.get("primaryCtaText").asText();
      String primaryCtaLink = maybeDecorateUrl(json.get("primaryCtaLink").asText(), clientId);

      productUpdate.setPrimaryCtaText(primaryCtaText);
      productUpdate.setPrimaryCtaLink(primaryCtaLink);
    }

    // Parse secondary CTA (optional)
    if (json.has("secondaryCtaText") && json.has("secondaryCtaLink")) {
      String secondaryCtaText = json.get("secondaryCtaText").asText();
      String secondaryCtaLink = maybeDecorateUrl(json.get("secondaryCtaLink").asText(), clientId);

      productUpdate.setSecondaryCtaText(secondaryCtaText);
      productUpdate.setSecondaryCtaLink(secondaryCtaLink);
    }

    // Parse legacy CTA fields (backward compatibility)
    // Only use if primary CTA is not provided
    if (!json.has("primaryCtaText") || !json.has("primaryCtaLink")) {
      String ctaText = json.has("ctaText") ? json.get("ctaText").asText() : "Learn more";
      String ctaLink =
          maybeDecorateUrl(json.has("ctaLink") ? json.get("ctaLink").asText() : "", clientId);

      productUpdate.setCtaText(ctaText);
      productUpdate.setCtaLink(ctaLink);
    }

    // Parse features array if present
    if (json.has("features") && json.get("features").isArray()) {
      List<ProductUpdateFeature> features = parseFeatures(json.get("features"));
      if (!features.isEmpty()) {
        productUpdate.setFeatures(features);
      }
    }

    return productUpdate;
  }

  /**
   * Parse features array from JSON.
   *
   * @param featuresArray JSON array node containing feature objects
   * @return List of parsed ProductUpdateFeature objects (may be empty)
   */
  @Nonnull
  private static List<ProductUpdateFeature> parseFeatures(@Nonnull JsonNode featuresArray) {
    List<ProductUpdateFeature> features = new ArrayList<>();

    for (JsonNode featureNode : featuresArray) {
      ProductUpdateFeature feature = parseFeature(featureNode);
      if (feature != null) {
        features.add(feature);
      }
    }

    return features;
  }

  /**
   * Parse a single feature from JSON.
   *
   * @param featureNode JSON node containing a feature object
   * @return Parsed ProductUpdateFeature, or null if parsing fails or required fields are missing
   */
  @Nullable
  private static ProductUpdateFeature parseFeature(@Nonnull JsonNode featureNode) {
    // Validate required fields
    if (!featureNode.has("title") || !featureNode.has("description")) {
      log.warn("Skipping invalid feature entry: missing required fields (title or description)");
      return null;
    }

    try {
      ProductUpdateFeature feature = new ProductUpdateFeature();
      feature.setTitle(featureNode.get("title").asText());
      feature.setDescription(featureNode.get("description").asText());

      // Icon is optional
      if (featureNode.has("icon")) {
        feature.setIcon(featureNode.get("icon").asText());
      }

      // Availability is optional
      if (featureNode.has("availability")) {
        feature.setAvailability(featureNode.get("availability").asText());
      }

      return feature;
    } catch (Exception e) {
      log.warn("Failed to parse feature entry, skipping: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Conditionally decorates a URL with clientId if the clientId is valid and URL is non-empty.
   *
   * @param url The URL to potentially decorate (may be empty)
   * @param clientId The client ID to append (may be null or empty)
   * @return The decorated URL if conditions are met, otherwise the original URL
   */
  @Nonnull
  private static String maybeDecorateUrl(@Nonnull String url, @Nullable String clientId) {
    if (clientId != null && !clientId.trim().isEmpty() && !url.isEmpty()) {
      return decorateUrlWithClientId(url, clientId);
    }
    return url;
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
