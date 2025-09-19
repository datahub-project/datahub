package utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class for handling base path operations in DataHub frontend. Provides methods to strip
 * and add base paths for proper routing.
 */
public class BasePathUtils {

  /**
   * Normalizes a base path by ensuring it starts with "/" and doesn't end with "/" (unless it's
   * root).
   *
   * @param basePath the raw base path from configuration
   * @return normalized base path
   */
  @Nonnull
  public static String normalizeBasePath(@Nullable String basePath) {
    if (basePath == null || basePath.trim().isEmpty() || basePath.equals("/")) {
      return "";
    }

    String normalized = basePath.trim();

    // Ensure it starts with /
    if (!normalized.startsWith("/")) {
      normalized = "/" + normalized;
    }

    // Remove trailing slash unless it's the root path
    if (normalized.length() > 1 && normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }

    return normalized;
  }

  /**
   * Strips the base path from a request URI if present.
   *
   * @param requestUri the full request URI
   * @param basePath the configured base path
   * @return the URI with base path stripped
   */
  @Nonnull
  public static String stripBasePath(@Nonnull String requestUri, @Nonnull String basePath) {
    String normalizedBasePath = normalizeBasePath(basePath);

    // If no base path configured, return original URI
    if (normalizedBasePath.isEmpty() || normalizedBasePath.endsWith("/")) {
      return requestUri;
    }

    // If the URI starts with the base path, strip it
    if (requestUri.startsWith(normalizedBasePath)) {
      String stripped = requestUri.substring(normalizedBasePath.length());
      // Ensure the result starts with /
      if (!stripped.startsWith("/")) {
        stripped = "/" + stripped;
      }
      return stripped;
    }

    return requestUri;
  }

  /**
   * Adds the base path to a URI if configured.
   *
   * @param uri the URI to prefix
   * @param basePath the configured base path
   * @return the URI with base path added
   */
  @Nonnull
  public static String addBasePath(@Nonnull String uri, @Nonnull String basePath) {
    String normalizedBasePath = normalizeBasePath(basePath);

    // If no base path configured, return original URI
    if (normalizedBasePath.isEmpty()) {
      return uri;
    }

    // If URI already has the base path, return as-is
    if (uri.startsWith(normalizedBasePath)) {
      return uri;
    }

    // Remove leading slash from URI if present to avoid double slashes
    String cleanUri = uri.startsWith("/") ? uri.substring(1) : uri;

    return normalizedBasePath + "/" + cleanUri;
  }

  /**
   * Checks if a request URI matches a specific route pattern, accounting for base path.
   *
   * @param requestUri the full request URI
   * @param routePattern the route pattern to match (e.g., "/login", "/api/v2/graphql")
   * @param basePath the configured base path
   * @return true if the URI matches the route pattern after base path stripping
   */
  public static boolean matchesRoute(
      @Nonnull String requestUri, @Nonnull String routePattern, @Nonnull String basePath) {
    String strippedUri = stripBasePath(requestUri, basePath);
    return strippedUri.equals(routePattern)
        || strippedUri.startsWith(routePattern + "/")
        || strippedUri.startsWith(routePattern + "?");
  }
}
