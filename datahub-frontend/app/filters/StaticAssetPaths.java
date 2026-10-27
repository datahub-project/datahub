package filters;

import com.typesafe.config.Config;

/**
 * Request-path helpers shared by the filters that act on files served by {@code
 * controllers.Assets}. Paths are matched after stripping {@code play.http.context} and {@code
 * datahub.basePath}, so {@code /datahub/assets/x.js} and {@code /assets/x.js} are the same asset.
 */
final class StaticAssetPaths {

  static final String ASSETS_PREFIX = "/assets/";
  static final String NODE_MODULES_PREFIX = "/node_modules/";
  static final String MANIFEST = "/manifest.json";

  private StaticAssetPaths() {}

  /**
   * True for every URL that conf/routes sends to {@code controllers.Assets.at}: {@code /assets/*},
   * {@code /node_modules/*} and {@code /manifest.json}.
   */
  static boolean isServedByAssets(String path, String httpContext, String basePath) {
    String relative = relativePath(path, httpContext, basePath);
    return relative != null
        && (relative.startsWith(ASSETS_PREFIX)
            || relative.startsWith(NODE_MODULES_PREFIX)
            || relative.equals(MANIFEST));
  }

  /**
   * The request path with its query, fragment, {@code play.http.context} and {@code
   * datahub.basePath} removed, or {@code null} for an empty path.
   */
  static String relativePath(String path, String httpContext, String basePath) {
    String normalized = stripQueryAndFragment(path);
    if (normalized == null) {
      return null;
    }
    return stripBase(
        stripBase(normalized, normalizeContext(httpContext)), normalizeContext(basePath));
  }

  static String normalizeContext(String raw) {
    if (raw == null) {
      return "";
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty() || "/".equals(trimmed)) {
      return "";
    }
    if (!trimmed.startsWith("/")) {
      trimmed = "/" + trimmed;
    }
    if (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  /** Reads a context setting such as {@code play.http.context}, normalized; root is {@code ""}. */
  static String configuredContext(Config config, String key) {
    if (config != null && config.hasPath(key)) {
      return normalizeContext(config.getString(key));
    }
    return "";
  }

  private static String stripQueryAndFragment(String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }
    int end = path.length();
    int query = path.indexOf('?');
    if (query >= 0) {
      end = query;
    }
    int fragment = path.indexOf('#');
    if (fragment >= 0 && fragment < end) {
      end = fragment;
    }
    return path.substring(0, end);
  }

  private static String stripBase(String path, String normalizedBase) {
    if (normalizedBase.isEmpty()) {
      return path;
    }
    if (path.equals(normalizedBase)) {
      return "/";
    }
    String prefix = normalizedBase + "/";
    if (path.startsWith(prefix)) {
      return path.substring(normalizedBase.length());
    }
    return path;
  }
}
