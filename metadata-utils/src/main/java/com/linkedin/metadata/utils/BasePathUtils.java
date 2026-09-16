package com.linkedin.metadata.utils;

import javax.annotation.Nullable;

/**
 * Utility class for resolving base paths based on enabled flag and path value. This consolidates
 * the base path resolution logic used across different modules.
 */
public class BasePathUtils {

  private BasePathUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Resolves the base path based on the enabled flag and path value.
   *
   * @param basePathEnabled whether base path is enabled
   * @param basePath the base path value
   * @return the resolved base path, or empty string if not enabled or invalid
   */
  public static String resolveBasePath(boolean basePathEnabled, String basePath) {
    return resolveBasePath(basePathEnabled, basePath, "");
  }

  /**
   * Resolves the base path based on the enabled flag and path value. Returns a default value when
   * base path is not enabled or invalid.
   *
   * @param basePathEnabled whether base path is enabled
   * @param basePath the base path value
   * @param defaultValue the default value to return when base path is not enabled or invalid
   * @return the resolved base path, or the default value if not enabled or invalid
   */
  public static String resolveBasePath(
      boolean basePathEnabled, String basePath, String defaultValue) {
    if (basePathEnabled && basePath != null && !basePath.isEmpty() && !"/".equals(basePath)) {
      return basePath;
    }
    return defaultValue;
  }

  /**
   * Resolves the base path based on the enabled flag and path value. This overload handles Boolean
   * objects that might be null.
   *
   * @param basePathEnabled whether base path is enabled (can be null)
   * @param basePath the base path value
   * @return the resolved base path, or empty string if not enabled or invalid
   */
  public static String resolveBasePath(Boolean basePathEnabled, String basePath) {
    return resolveBasePath(basePathEnabled, basePath, "");
  }

  /**
   * Resolves the base path based on the enabled flag and path value. Returns a default value when
   * base path is not enabled or invalid. This overload handles Boolean objects that might be null.
   *
   * @param basePathEnabled whether base path is enabled (can be null)
   * @param basePath the base path value
   * @param defaultValue the default value to return when base path is not enabled or invalid
   * @return the resolved base path, or the default value if not enabled or invalid
   */
  public static String resolveBasePath(
      Boolean basePathEnabled, String basePath, String defaultValue) {
    if (basePathEnabled != null
        && basePathEnabled
        && basePath != null
        && !basePath.isEmpty()
        && !"/".equals(basePath)) {
      return basePath;
    }
    return defaultValue;
  }

  /**
   * Normalizes a base path by ensuring it starts with a slash and doesn't end with a slash.
   *
   * @param basePath the base path to normalize
   * @return the normalized base path, or empty string if null/empty
   */
  public static String normalizeBasePath(@Nullable String basePath) {
    if (basePath == null || basePath.isEmpty()) {
      return "";
    }

    // Remove trailing slash
    String normalized =
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;

    // Ensure it starts with a slash
    if (!normalized.isEmpty() && !normalized.startsWith("/")) {
      normalized = "/" + normalized;
    }

    return normalized;
  }

  /**
   * Adds a base path to a given path, handling proper slash concatenation.
   *
   * @param path the path to add the base path to
   * @param basePath the base path to add
   * @return the combined path
   */
  public static String addBasePath(String path, @Nullable String basePath) {
    if (basePath == null || basePath.isEmpty()) {
      return path;
    }

    String normalizedBasePath = normalizeBasePath(basePath);
    if (normalizedBasePath.isEmpty()) {
      return path;
    }

    // Ensure path starts with a slash
    String normalizedPath = path.startsWith("/") ? path : "/" + path;

    return normalizedBasePath + normalizedPath;
  }

  /**
   * Strips the base path from a given path.
   *
   * @param path the full path
   * @param basePath the base path to strip
   * @return the path with base path stripped, or original path if base path is not found
   */
  public static String stripBasePath(String path, @Nullable String basePath) {
    if (basePath == null || basePath.isEmpty()) {
      return path;
    }

    String normalizedBasePath = normalizeBasePath(basePath);
    if (normalizedBasePath.isEmpty()) {
      return path;
    }

    if (path.startsWith(normalizedBasePath)) {
      String stripped = path.substring(normalizedBasePath.length());
      // Ensure the result starts with a slash if it's not empty
      return stripped.isEmpty() ? "/" : (stripped.startsWith("/") ? stripped : "/" + stripped);
    }

    return path;
  }
}
