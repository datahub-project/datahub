package com.linkedin.datahub.graphql.resolvers.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.datahub.graphql.generated.ProductUpdate;
import com.linkedin.datahub.graphql.generated.ProductUpdateFeature;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
    return parseProductUpdate(jsonOpt, clientId, null);
  }

  /**
   * Parse JSON into a ProductUpdate object, decorating CTA links with clientId and overlaying
   * locale-specific copy when present.
   *
   * <p>Translated strings live under an optional {@code i18n} object keyed by locale ({@code ja},
   * {@code pt-BR}, …). Each locale object may include any of {@code title}, {@code header}, {@code
   * description}, {@code primaryCtaText}, {@code secondaryCtaText}, {@code ctaText}, and {@code
   * features} (title/description/availability by index). Missing keys keep the default English
   * fields. Links, {@code id}, {@code enabled}, {@code image}, and {@code requiredVersion} are
   * never translated.
   *
   * @param jsonOpt Optional JSON node containing product update data
   * @param clientId Optional client ID to append to ctaLink as a query parameter
   * @param locale Optional UI locale (e.g. {@code ja} or {@code ja-JP}); {@code ja-JP} falls back
   *     to {@code ja}
   * @return ProductUpdate object if parsing succeeds and update is enabled, null otherwise
   */
  @Nullable
  public static ProductUpdate parseProductUpdate(
      @Nonnull Optional<JsonNode> jsonOpt, @Nullable String clientId, @Nullable String locale) {
    if (jsonOpt.isEmpty()) {
      log.debug("No product update JSON available");
      return null;
    }

    JsonNode json = jsonOpt.get();

    // Parse and validate required fields
    if (!json.has("enabled") || !json.has("id")) {
      log.warn("Product update JSON missing required fields (enabled or id)");
      return null;
    }

    boolean enabled = json.get("enabled").asBoolean();
    if (!enabled) {
      log.debug("Product update is disabled in JSON");
      return null;
    }

    String id = json.get("id").asText();
    String title = json.hasNonNull("title") ? json.get("title").asText() : "";

    // Build the ProductUpdate response
    ProductUpdate productUpdate = new ProductUpdate();
    productUpdate.setEnabled(enabled);
    productUpdate.setId(id);
    productUpdate.setTitle(title);

    // Optional fields
    if (json.hasNonNull("releaseMonth")) {
      productUpdate.setReleaseMonth(json.get("releaseMonth").asText());
    }
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
    boolean hasPrimaryCta = json.hasNonNull("primaryCtaText") && json.hasNonNull("primaryCtaLink");
    if (hasPrimaryCta) {
      String primaryCtaText = json.get("primaryCtaText").asText();
      String primaryCtaLink = maybeDecorateUrl(json.get("primaryCtaLink").asText(), clientId);

      productUpdate.setPrimaryCtaText(primaryCtaText);
      productUpdate.setPrimaryCtaLink(primaryCtaLink);
    }

    // Parse secondary CTA (optional)
    if (json.hasNonNull("secondaryCtaText") && json.hasNonNull("secondaryCtaLink")) {
      String secondaryCtaText = json.get("secondaryCtaText").asText();
      String secondaryCtaLink = maybeDecorateUrl(json.get("secondaryCtaLink").asText(), clientId);

      productUpdate.setSecondaryCtaText(secondaryCtaText);
      productUpdate.setSecondaryCtaLink(secondaryCtaLink);
    }

    // Keep deprecated non-null GraphQL fields populated for backward compatibility. Empty values
    // signal the frontend to use its localized defaults when neither CTA format is provided.
    String ctaText = json.hasNonNull("ctaText") ? json.get("ctaText").asText() : "";
    String ctaLink =
        maybeDecorateUrl(json.hasNonNull("ctaLink") ? json.get("ctaLink").asText() : "", clientId);
    productUpdate.setCtaText(ctaText);
    productUpdate.setCtaLink(ctaLink);

    // Parse features array if present
    if (json.has("features") && json.get("features").isArray()) {
      List<ProductUpdateFeature> features = parseFeatures(json.get("features"));
      if (!features.isEmpty()) {
        productUpdate.setFeatures(features);
      }
    }

    applyI18nOverlay(productUpdate, json, locale);
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

  /**
   * Overlay locale-specific copy onto an already-parsed ProductUpdate. No-op when locale is absent
   * or the JSON has no matching {@code i18n} entry.
   */
  private static void applyI18nOverlay(
      @Nonnull ProductUpdate productUpdate, @Nonnull JsonNode json, @Nullable String locale) {
    JsonNode override = findI18nOverride(json, locale);
    if (override == null) {
      return;
    }

    localizedText(override, "title").ifPresent(productUpdate::setTitle);
    localizedText(override, "header").ifPresent(productUpdate::setHeader);
    localizedText(override, "description").ifPresent(productUpdate::setDescription);
    localizedText(override, "primaryCtaText").ifPresent(productUpdate::setPrimaryCtaText);
    localizedText(override, "secondaryCtaText").ifPresent(productUpdate::setSecondaryCtaText);
    localizedText(override, "ctaText").ifPresent(productUpdate::setCtaText);
    overlayFeatures(productUpdate, override);
  }

  @Nullable
  private static JsonNode findI18nOverride(@Nonnull JsonNode json, @Nullable String locale) {
    if (locale == null || locale.isBlank()) {
      return null;
    }
    JsonNode i18n = json.get("i18n");
    if (i18n == null || !i18n.isObject()) {
      return null;
    }

    String requested = locale.trim();
    JsonNode match = getCaseInsensitiveObject(i18n, requested);
    if (match != null) {
      return match;
    }

    String language = languageSubtag(requested);
    if (!language.equalsIgnoreCase(requested)) {
      return getCaseInsensitiveObject(i18n, language);
    }
    return null;
  }

  @Nullable
  private static JsonNode getCaseInsensitiveObject(@Nonnull JsonNode i18n, @Nonnull String key) {
    JsonNode direct = i18n.get(key);
    if (isObject(direct)) {
      return direct;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    Iterator<Map.Entry<String, JsonNode>> fields = i18n.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      if (entry.getKey().toLowerCase(Locale.ROOT).equals(lower) && isObject(entry.getValue())) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Nonnull
  private static String languageSubtag(@Nonnull String locale) {
    int dash = locale.indexOf('-');
    int underscore = locale.indexOf('_');
    int cut;
    if (dash < 0) {
      cut = underscore;
    } else if (underscore < 0) {
      cut = dash;
    } else {
      cut = Math.min(dash, underscore);
    }
    return cut > 0 ? locale.substring(0, cut) : locale;
  }

  private static boolean isObject(@Nullable JsonNode node) {
    return node != null && node.isObject();
  }

  @Nonnull
  private static Optional<String> localizedText(@Nonnull JsonNode parent, @Nonnull String field) {
    JsonNode node = parent.get(field);
    if (node == null || !node.isTextual()) {
      return Optional.empty();
    }
    String text = node.asText();
    if (text.isBlank() || "null".equals(text)) {
      return Optional.empty();
    }
    return Optional.of(text);
  }

  private static void overlayFeatures(
      @Nonnull ProductUpdate productUpdate, @Nonnull JsonNode override) {
    List<ProductUpdateFeature> features = productUpdate.getFeatures();
    if (features == null || features.isEmpty()) {
      return;
    }
    JsonNode featuresNode = override.get("features");
    if (featuresNode == null || !featuresNode.isArray()) {
      return;
    }
    int count = Math.min(features.size(), featuresNode.size());
    for (int i = 0; i < count; i++) {
      JsonNode featureOverride = featuresNode.get(i);
      if (!isObject(featureOverride)) {
        continue;
      }
      ProductUpdateFeature feature = features.get(i);
      localizedText(featureOverride, "title").ifPresent(feature::setTitle);
      localizedText(featureOverride, "description").ifPresent(feature::setDescription);
      localizedText(featureOverride, "availability").ifPresent(feature::setAvailability);
    }
  }
}
