package filters;

import com.typesafe.config.Config;
import java.util.Locale;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.core.Execution;
import play.mvc.EssentialAction;
import play.mvc.EssentialFilter;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Serves Vite content-hashed files with a one-year immutable cache.
 *
 * <p>Play's {@code Assets.at} default is {@code public, max-age=3600}. That is correct for Monaco
 * under {@code /node_modules} and for stable URLs under {@code /assets} (platform logos, theme
 * JSON, the connector registry, the favicon, {@code logo.png}). It is wasted work for files Vite
 * emits as {@code /assets/<name>-<8 char hash>.<ext>}: the name changes whenever the bytes change,
 * so a returning browser can skip revalidation.
 *
 * <p>{@code play.assets.defaultCache} and {@code play.assets.cache."/public/assets/"} are not used.
 * Both would also cover those stable URLs, and a year-long immutable directive there cannot be
 * busted until the URL changes.
 *
 * <p>Stable files belong in a subdirectory of {@code /assets/}. A file placed directly under {@code
 * /assets/} whose name happens to end in {@code -<8 base64url chars>.<ext>} is treated as hashed.
 *
 * <p>Source maps are not. Rollup names a map after the chunk ({@code <chunk>-<hash>.js.map}), so
 * the hash is of the chunk rather than the map. A later build can change the map without changing
 * that URL. Maps stay on the one-hour default.
 */
@Singleton
public class ContentHashedAssetCacheFilter extends EssentialFilter {

  /** Matches Play's {@code play.assets.aggressiveCache} (one year). */
  static final String IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable";

  /**
   * Rollup's default {@code [hash]} placeholder is 8 characters of base64url ({@code A-Za-z0-9_-}).
   * Hex hashes of the same length are a subset.
   */
  private static final Pattern VITE_CONTENT_HASH =
      Pattern.compile("^.+-[A-Za-z0-9_-]{8}(?:\\.[A-Za-z0-9]+)+$");

  private final String httpContext;
  private final String basePath;

  @Inject
  public ContentHashedAssetCacheFilter(Config config) {
    this.httpContext = normalizeContext(configuredPath(config, "play.http.context"));
    this.basePath = normalizeContext(configuredPath(config, "datahub.basePath"));
  }

  @Override
  public EssentialAction apply(EssentialAction next) {
    return EssentialAction.of(
        request ->
            next.apply(request)
                .map(result -> applyCacheHeader(request.path(), result), Execution.trampoline()));
  }

  Result applyCacheHeader(String path, Result result) {
    return withImmutableCacheIfHashed(path, httpContext, basePath, result);
  }

  static Result withImmutableCacheIfHashed(
      String path, String httpContext, String basePath, Result result) {
    if (!isCacheableSuccess(result.status()) || isHtml(result)) {
      return result;
    }
    if (!isContentHashedAssetPath(path, httpContext, basePath)) {
      return result;
    }
    return result.withHeader(Http.HeaderNames.CACHE_CONTROL, IMMUTABLE_CACHE_CONTROL);
  }

  static boolean isContentHashedAssetPath(String path, String httpContext, String basePath) {
    String fileName = assetFileName(path, httpContext, basePath);
    return fileName != null
        && !isSourceMap(fileName)
        && VITE_CONTENT_HASH.matcher(fileName).matches();
  }

  /**
   * {@code .map.gz} and {@code .map.br} are the precompressed sidecars. Play usually negotiates
   * those from the {@code .map} URL, but a direct request for the sidecar is the same bytes.
   */
  private static boolean isSourceMap(String fileName) {
    String lower = fileName.toLowerCase(Locale.ROOT);
    return lower.endsWith(".map") || lower.endsWith(".map.gz") || lower.endsWith(".map.br");
  }

  private static String assetFileName(String path, String httpContext, String basePath) {
    String normalized = stripQueryAndFragment(path);
    if (normalized == null) {
      return null;
    }
    normalized =
        stripBase(stripBase(normalized, normalizeContext(httpContext)), normalizeContext(basePath));
    final String prefix = "/assets/";
    if (!normalized.startsWith(prefix)) {
      return null;
    }
    String fileName = normalized.substring(prefix.length());
    if (fileName.isEmpty() || fileName.indexOf('/') >= 0 || fileName.indexOf('\\') >= 0) {
      return null;
    }
    return fileName;
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

  private static String configuredPath(Config config, String key) {
    if (config != null && config.hasPath(key)) {
      return config.getString(key);
    }
    return "/";
  }

  private static boolean isCacheableSuccess(int status) {
    return status == Http.Status.OK
        || status == Http.Status.PARTIAL_CONTENT
        || status == Http.Status.NOT_MODIFIED;
  }

  private static boolean isHtml(Result result) {
    return isHtmlType(result.contentType().orElse(null))
        || isHtmlType(result.headers().get(Http.HeaderNames.CONTENT_TYPE));
  }

  private static boolean isHtmlType(String contentType) {
    if (contentType == null || contentType.isEmpty()) {
      return false;
    }
    String mime = contentType.toLowerCase(Locale.ROOT);
    int separator = mime.indexOf(';');
    if (separator >= 0) {
      mime = mime.substring(0, separator).trim();
    }
    return "text/html".equals(mime);
  }
}
