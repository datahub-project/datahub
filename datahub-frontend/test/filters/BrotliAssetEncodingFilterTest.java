package filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import play.libs.streams.Accumulator;
import play.mvc.EssentialAction;
import play.mvc.Http;
import play.mvc.Results;

class BrotliAssetEncodingFilterTest {

  private static final String HASHED_JS = "/assets/index-K056Uwn8.js";

  @Test
  void brotliCodings_areRemoved() {
    assertEquals("gzip", rewrite("br, gzip"));
    assertEquals("gzip", rewrite("gzip, br"));
    assertEquals("gzip, deflate, zstd", rewrite("gzip, deflate, br, zstd"));
    assertEquals("gzip;q=0.8", rewrite("br;q=1.0, gzip;q=0.8"));
    assertEquals("gzip", rewrite("BR,gzip"));
    assertEquals("gzip", rewrite("  br ; q=0.9 ,  gzip  "));
    assertEquals("gzip, identity;q=0.5", rewrite("br", "gzip, identity;q=0.5"));
  }

  @Test
  void brotliOnly_fallsBackToIdentity() {
    assertEquals("identity", rewrite("br"));
    assertEquals("identity", rewrite("br;q=1"));
    assertEquals("identity", rewrite(" br , "));
  }

  @Test
  void wildcard_excludesBrotliExplicitly() {
    assertEquals("*, br;q=0", rewrite("*"));
    assertEquals("gzip, *;q=0.1, br;q=0", rewrite("br, gzip, *;q=0.1"));
  }

  @Test
  void headersWithoutBrotli_areLeftAlone() {
    assertNull(rewrite("gzip"));
    assertNull(rewrite("gzip, deflate"));
    assertNull(rewrite("identity"));
    // Codings that only start with "br" are not Brotli.
    assertNull(rewrite("brotli, gzip"));
    assertNull(BrotliAssetEncodingFilter.acceptEncodingWithoutBrotli(List.of()));
    assertNull(BrotliAssetEncodingFilter.acceptEncodingWithoutBrotli(null));
  }

  @Test
  void enabled_isANoOp() {
    BrotliAssetEncodingFilter filter = filter(true, "/");
    EssentialAction next = EssentialAction.of(request -> Accumulator.done(Results.ok()));

    assertSame(next, filter.apply(next));
  }

  @Test
  void enabledByDefault_whenTheSettingIsAbsent() {
    BrotliAssetEncodingFilter filter = new BrotliAssetEncodingFilter(ConfigFactory.empty());
    EssentialAction next = EssentialAction.of(request -> Accumulator.done(Results.ok()));

    assertSame(next, filter.apply(next));
  }

  @Test
  void disabled_rewritesAssetRequests() {
    BrotliAssetEncodingFilter filter = filter(false, "/");

    assertEquals(
        "gzip, deflate", acceptEncodingSeenByAction(filter, HASHED_JS, "br, gzip, deflate"));
    assertEquals("identity", acceptEncodingSeenByAction(filter, HASHED_JS, "br"));
    assertEquals(
        "gzip",
        acceptEncodingSeenByAction(
            filter, "/node_modules/monaco-editor/min/vs/loader.js", "gzip, br"));
    assertEquals("gzip", acceptEncodingSeenByAction(filter, "/manifest.json", "br, gzip"));
    assertEquals(
        "gzip",
        acceptEncodingSeenByAction(filter, "/assets/platforms/snowflakelogo.png", "br, gzip"));
  }

  @Test
  void disabled_leavesOtherRequestsAlone() {
    BrotliAssetEncodingFilter filter = filter(false, "/");

    assertEquals("br, gzip", acceptEncodingSeenByAction(filter, "/", "br, gzip"));
    assertEquals("br, gzip", acceptEncodingSeenByAction(filter, "/api/v2/graphql", "br, gzip"));
    assertEquals(
        "br, gzip",
        acceptEncodingSeenByAction(filter, "/api/assets/index-K056Uwn8.js", "br, gzip"));
    assertEquals("br, gzip", acceptEncodingSeenByAction(filter, "/manifest.json.br", "br, gzip"));
    assertEquals("gzip", acceptEncodingSeenByAction(filter, HASHED_JS, "gzip"));
  }

  @Test
  void disabled_withoutAcceptEncoding_addsNothing() {
    BrotliAssetEncodingFilter filter = filter(false, "/");
    Http.RequestHeader request = new Http.RequestBuilder().method("GET").uri(HASHED_JS).build();

    assertSame(request, filter.withoutBrotli(request));
  }

  @Test
  void disabled_stripsConfiguredBasePath() {
    BrotliAssetEncodingFilter filter = filter(false, "/datahub");

    assertEquals(
        "gzip",
        acceptEncodingSeenByAction(filter, "/datahub/assets/index-K056Uwn8.js", "br, gzip"));
    assertEquals("br, gzip", acceptEncodingSeenByAction(filter, "/datahub/", "br, gzip"));
  }

  @Test
  void otherRequestHeaders_arePreserved() {
    BrotliAssetEncodingFilter filter = filter(false, "/");
    Http.RequestHeader request =
        new Http.RequestBuilder()
            .method("GET")
            .uri(HASHED_JS)
            .header(Http.HeaderNames.ACCEPT_ENCODING, "br, gzip")
            .header(Http.HeaderNames.IF_NONE_MATCH, "\"abc\"")
            .build();

    Http.RequestHeader rewritten = filter.withoutBrotli(request);

    assertEquals("\"abc\"", rewritten.header(Http.HeaderNames.IF_NONE_MATCH).orElse(null));
    assertEquals(HASHED_JS, rewritten.path());
    assertEquals(List.of("gzip"), rewritten.headers().getAll(Http.HeaderNames.ACCEPT_ENCODING));
  }

  private static String rewrite(String... headerValues) {
    return BrotliAssetEncodingFilter.acceptEncodingWithoutBrotli(List.of(headerValues));
  }

  private static BrotliAssetEncodingFilter filter(boolean brotliEnabled, String basePath) {
    Map<String, Object> settings = new HashMap<>();
    settings.put(BrotliAssetEncodingFilter.BROTLI_ENABLED_CONFIG_PATH, brotliEnabled);
    settings.put("play.http.context", basePath);
    settings.put("datahub.basePath", basePath);
    Config config = ConfigFactory.parseMap(settings);
    return new BrotliAssetEncodingFilter(config);
  }

  private static String acceptEncodingSeenByAction(
      BrotliAssetEncodingFilter filter, String path, String acceptEncoding) {
    AtomicReference<Http.RequestHeader> seen = new AtomicReference<>();
    EssentialAction next =
        EssentialAction.of(
            request -> {
              seen.set(request);
              return Accumulator.done(Results.ok());
            });
    Http.RequestHeader request =
        new Http.RequestBuilder()
            .method("GET")
            .uri(path)
            .header(Http.HeaderNames.ACCEPT_ENCODING, acceptEncoding)
            .build();

    filter.apply(next).apply(request);

    return String.join(", ", seen.get().headers().getAll(Http.HeaderNames.ACCEPT_ENCODING));
  }
}
