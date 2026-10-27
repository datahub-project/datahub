package filters;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.EssentialAction;
import play.mvc.EssentialFilter;
import play.mvc.Http;

/**
 * Stops Play Assets from serving the Brotli ({@code .br}) sidecars when {@code
 * datahub.assets.brotli.enabled} is false.
 *
 * <p>Some TLS-inspecting proxies drop or rewrite {@code Content-Encoding: br}, so the browser gets
 * Brotli bytes it does not decode and the JavaScript chunk fails to load. Turning the setting off
 * removes the {@code br} coding from {@code Accept-Encoding} on requests that {@code
 * controllers.Assets} serves. Play then picks the {@code .gz} sidecar, or the uncompressed file if
 * the client accepted only {@code br}. Every other request is passed through unchanged, and the
 * filter does nothing while the setting is true.
 *
 * <p>Play Assets still sets {@code Vary: Accept-Encoding} on files that have sidecars, so a shared
 * cache keyed on the original header stays correct.
 */
@Singleton
public class BrotliAssetEncodingFilter extends EssentialFilter {

  static final String BROTLI_ENABLED_CONFIG_PATH = "datahub.assets.brotli.enabled";

  private static final Logger logger = LoggerFactory.getLogger(BrotliAssetEncodingFilter.class);
  private static final String BROTLI = "br";
  private static final String WILDCARD = "*";
  private static final String IDENTITY = "identity";

  private final boolean brotliEnabled;
  private final String httpContext;
  private final String basePath;

  @Inject
  public BrotliAssetEncodingFilter(Config config) {
    this.brotliEnabled =
        !config.hasPath(BROTLI_ENABLED_CONFIG_PATH)
            || config.getBoolean(BROTLI_ENABLED_CONFIG_PATH);
    this.httpContext = StaticAssetPaths.configuredContext(config, "play.http.context");
    this.basePath = StaticAssetPaths.configuredContext(config, "datahub.basePath");
    if (!brotliEnabled) {
      logger.info("Brotli static assets are disabled; serving gzip or identity encodings only");
    }
  }

  @Override
  public EssentialAction apply(EssentialAction next) {
    if (brotliEnabled) {
      return next;
    }
    return EssentialAction.of(request -> next.apply(withoutBrotli(request)));
  }

  Http.RequestHeader withoutBrotli(Http.RequestHeader request) {
    if (!StaticAssetPaths.isServedByAssets(request.path(), httpContext, basePath)) {
      return request;
    }
    List<String> acceptEncoding = request.headers().getAll(Http.HeaderNames.ACCEPT_ENCODING);
    String rewritten = acceptEncodingWithoutBrotli(acceptEncoding);
    if (rewritten == null) {
      return request;
    }
    Http.Headers headers =
        request
            .headers()
            .removing(Http.HeaderNames.ACCEPT_ENCODING)
            .adding(Http.HeaderNames.ACCEPT_ENCODING, rewritten);
    return request.asScala().withHeaders(headers.asScala()).asJava();
  }

  /**
   * Returns the {@code Accept-Encoding} value with every {@code br} coding removed, or {@code null}
   * when the header has nothing to remove.
   *
   * <p>Codings are matched case-insensitively and with their parameters ({@code br;q=0.9}). A
   * remaining {@code *} would still cover Brotli, so {@code br;q=0} is appended to exclude it. If
   * {@code br} was the only coding, the result is {@code identity}.
   */
  static String acceptEncodingWithoutBrotli(List<String> headerValues) {
    if (headerValues == null || headerValues.isEmpty()) {
      return null;
    }
    List<String> kept = new ArrayList<>();
    boolean removed = false;
    boolean wildcard = false;
    for (String headerValue : headerValues) {
      for (String coding : headerValue.split(",")) {
        String trimmed = coding.trim();
        if (trimmed.isEmpty()) {
          continue;
        }
        String name = codingName(trimmed);
        if (BROTLI.equalsIgnoreCase(name)) {
          removed = true;
          continue;
        }
        wildcard |= WILDCARD.equals(name);
        kept.add(trimmed);
      }
    }
    if (wildcard) {
      kept.add(BROTLI + ";q=0");
    } else if (!removed) {
      return null;
    } else if (kept.isEmpty()) {
      return IDENTITY;
    }
    return String.join(", ", kept);
  }

  private static String codingName(String coding) {
    int separator = coding.indexOf(';');
    return separator >= 0 ? coding.substring(0, separator).trim() : coding;
  }
}
