package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Inlines the hosted product-update JSON into index.html so the toast can paint with the first
 * commit. The image URL stays in that JSON, which is how the announcement changes without a
 * frontend build.
 */
public final class ProductUpdateBootstrap {
  public static final String PLACEHOLDER = "@productUpdateBootstrap";
  static final String WINDOW_KEY = "__DATAHUB_PRODUCT_UPDATE__";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ProductUpdateBootstrap() {}

  @Nonnull
  public static String snippet(@Nullable String jsonBody) {
    if (jsonBody == null || jsonBody.isBlank()) {
      return "";
    }
    try {
      return snippet(OBJECT_MAPPER.readTree(jsonBody));
    } catch (Exception e) {
      return "";
    }
  }

  @Nonnull
  public static String snippet(@Nullable JsonNode json) {
    if (json == null || !json.path("enabled").asBoolean(false) || !json.hasNonNull("id")) {
      return "";
    }
    String encoded =
        json.toString().replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029");
    StringBuilder html = new StringBuilder();
    html.append("<script>window.").append(WINDOW_KEY).append("=").append(encoded).append(";</script>");
    String image = json.path("image").asText("");
    if (isPreloadableImage(image)) {
      html.append("<link rel=\"preload\" as=\"image\" href=\"")
          .append(escapeAttribute(image))
          .append("\" fetchpriority=\"high\">");
    }
    return html.toString();
  }

  private static boolean isPreloadableImage(String image) {
    return image.startsWith("https://")
        || image.startsWith("http://")
        || image.startsWith("/")
        || image.startsWith("assets/");
  }

  private static String escapeAttribute(String value) {
    return value.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;");
  }
}
