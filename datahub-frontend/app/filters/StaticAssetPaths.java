package filters;

/**
 * Paths whose bodies are pre-compressed at Vite build time ({@code .br} / {@code .gz} sidecars).
 * GzipFilter must not wrap these or Play/Envoy would double-compress.
 */
public final class StaticAssetPaths {
  private StaticAssetPaths() {}

  public static boolean isPrecompressedStaticPath(String path) {
    if (path == null || path.isEmpty()) {
      return false;
    }
    return path.contains("/assets/") || path.endsWith("/assets") || path.contains("/node_modules/");
  }
}
