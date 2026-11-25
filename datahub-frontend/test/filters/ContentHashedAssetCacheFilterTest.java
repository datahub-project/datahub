package filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Results;

class ContentHashedAssetCacheFilterTest {

  private static final String ONE_HOUR = "public, max-age=3600";

  @Test
  void hashedBuildOutputs_areImmutable() {
    assertImmutable("/assets/source-By3zWfCx.js");
    assertImmutable("/assets/index-K056Uwn8.js");
    assertImmutable("/assets/index-Byck28hc.css");
    assertImmutable("/assets/codicon-AbCdEf12.woff2");
    assertImmutable("/assets/source-By3zWfCx.js?unused=1");
    // Real Vite output: the hash itself may contain '-' or '_'.
    assertImmutable("/assets/pt-BR--e9KCM03.js");
    assertImmutable("/assets/index-D-jfAKOd.js");
    assertImmutable("/assets/EditorImpl-U8MXg4N_.js");
    assertImmutable("/datahub/assets/source-By3zWfCx.js", "/datahub", "/datahub");
  }

  @Test
  void stableAssetUrls_keepTheOneHourDefault() {
    // The hash in a source map's name belongs to the chunk, not the map.
    assertRevalidates("/assets/source-By3zWfCx.js.map");
    assertRevalidates("/assets/index-Byck28hc.css.map");
    assertRevalidates("/assets/source-By3zWfCx.js.map.gz");
    assertRevalidates("/assets/source-By3zWfCx.js.map.br");
    assertRevalidates("/assets/logo.png");
    assertRevalidates("/assets/platforms/snowflakelogo.png");
    assertRevalidates("/assets/platforms/incident-critical.svg");
    assertRevalidates("/assets/platforms/tableau-workbook.svg");
    assertRevalidates("/assets/conf/theme/ant_theme_v2.json");
    assertRevalidates("/assets/icons/favicon.ico");
    assertRevalidates("/assets/logos/datahub-logo.png");
    assertRevalidates("/assets/ingestion/connector_registry/manifest.json");
    assertRevalidates("/assets/ingestion/connector_registry/bigquery.json");
    assertRevalidates("/datahub/assets/platforms/snowflakelogo.png", "/datahub", "/datahub");
  }

  @Test
  void filenamesThatLookHashed_areOnlyImmutableDirectlyUnderAssets() {
    // "critical" and "workbook" are 8 letters, which is also a legal Vite hash. These files are
    // copied to /assets/platforms/, so they must keep revalidating. The same name directly under
    // /assets/ is the hashed-build shape and is immutable.
    assertTrue(
        ContentHashedAssetCacheFilter.isContentHashedAssetPath(
            "/assets/incident-critical.svg", "", ""));
    assertFalse(
        ContentHashedAssetCacheFilter.isContentHashedAssetPath(
            "/assets/platforms/incident-critical.svg", "", ""));
  }

  @Test
  void nodeModulesMonaco_isUnchanged() {
    assertRevalidates("/node_modules/monaco-editor/min/vs/loader.js");
    assertRevalidates("/node_modules/monaco-editor/min/vs/editor/editor.main.js");
    assertRevalidates(
        "/datahub/node_modules/monaco-editor/min/vs/loader.js", "/datahub", "/datahub");
  }

  @Test
  void indexHtml_staysNoCache() {
    Result index =
        Results.ok("<html></html>")
            .as("text/html")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, "no-cache");
    assertUnchanged("/", index);
    assertUnchanged("/browse/dataset", index);
    assertUnchanged("/index.html", index);
    assertUnchanged("/datahub/", index, "/datahub", "/datahub");

    Result htmlWithCharset =
        Results.ok("<html></html>")
            .withHeader(Http.HeaderNames.CONTENT_TYPE, "text/html; charset=utf-8")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, "no-cache");
    assertUnchanged("/assets/source-By3zWfCx.js", htmlWithCharset);
  }

  @Test
  void manifestAndApiPaths_areNotAssetCaches() {
    assertRevalidates("/manifest.json");
    assertRevalidates("/api/assets/source-By3zWfCx.js");
    assertRevalidates("/openapi/assets/source-By3zWfCx.js");
    assertRevalidates("/datahub/api/assets/source-By3zWfCx.js", "/datahub", "/datahub");
  }

  @Test
  void missingHashedAsset_isNotCachedAsImmutable() {
    Result missing =
        Results.notFound("missing")
            .as("application/javascript")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);
    Result updated =
        ContentHashedAssetCacheFilter.withImmutableCacheIfHashed(
            "/assets/missing-By3zWfCx.js", "", "", missing);
    assertEquals(Http.Status.NOT_FOUND, updated.status());
    assertEquals(ONE_HOUR, updated.headers().get(Http.HeaderNames.CACHE_CONTROL));
  }

  @Test
  void notModifiedAndPartialResponses_areImmutable() {
    Result notModified =
        Results.status(Http.Status.NOT_MODIFIED)
            .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);
    Result partial =
        Results.status(Http.Status.PARTIAL_CONTENT)
            .as("application/javascript")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);

    assertEquals(
        ContentHashedAssetCacheFilter.IMMUTABLE_CACHE_CONTROL,
        ContentHashedAssetCacheFilter.withImmutableCacheIfHashed(
                "/assets/source-By3zWfCx.js", "", "", notModified)
            .headers()
            .get(Http.HeaderNames.CACHE_CONTROL));
    assertEquals(
        ContentHashedAssetCacheFilter.IMMUTABLE_CACHE_CONTROL,
        ContentHashedAssetCacheFilter.withImmutableCacheIfHashed(
                "/assets/source-By3zWfCx.js", "", "", partial)
            .headers()
            .get(Http.HeaderNames.CACHE_CONTROL));
  }

  @Test
  void immutableHeader_replacesTheOneHourDefaultAndKeepsOtherHeaders() {
    Result original =
        Results.ok("payload")
            .as("application/javascript")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR)
            .withHeader(Http.HeaderNames.ETAG, "\"813708d95904218e140c4bee963281894aa15ea8\"");

    Result updated =
        ContentHashedAssetCacheFilter.withImmutableCacheIfHashed(
            "/assets/source-By3zWfCx.js", "", "", original);

    assertEquals(
        ContentHashedAssetCacheFilter.IMMUTABLE_CACHE_CONTROL,
        updated.headers().get(Http.HeaderNames.CACHE_CONTROL));
    assertEquals(
        "\"813708d95904218e140c4bee963281894aa15ea8\"",
        updated.headers().get(Http.HeaderNames.ETAG));
  }

  @Test
  void configuredBasePath_isStrippedBeforeMatching() {
    Config config =
        ConfigFactory.parseMap(
            Map.of("play.http.context", "/datahub", "datahub.basePath", "/datahub"));
    ContentHashedAssetCacheFilter filter = new ContentHashedAssetCacheFilter(config);

    Result hashed =
        Results.ok("payload")
            .as("application/javascript")
            .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);
    assertEquals(
        ContentHashedAssetCacheFilter.IMMUTABLE_CACHE_CONTROL,
        filter
            .applyCacheHeader("/datahub/assets/source-By3zWfCx.js", hashed)
            .headers()
            .get(Http.HeaderNames.CACHE_CONTROL));

    Result logo =
        Results.ok("png").as("image/png").withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);
    assertEquals(
        ONE_HOUR,
        filter
            .applyCacheHeader("/datahub/assets/logo.png", logo)
            .headers()
            .get(Http.HeaderNames.CACHE_CONTROL));
  }

  @Test
  void checkedInStableAssets_areNotClassifiedAsHashed() throws IOException {
    Path publicAssets = repoPath("datahub-web-react/public/assets");
    try (Stream<Path> files = Files.walk(publicAssets)) {
      files
          .filter(Files::isRegularFile)
          .forEach(
              file -> {
                String relative = publicAssets.relativize(file).toString().replace('\\', '/');
                assertFalse(
                    ContentHashedAssetCacheFilter.isContentHashedAssetPath(
                        "/assets/" + relative, "", ""),
                    relative);
              });
    }

    Path images = repoPath("datahub-web-react/src/images");
    try (Stream<Path> files = Files.list(images)) {
      files
          .filter(Files::isRegularFile)
          .forEach(
              file ->
                  assertFalse(
                      ContentHashedAssetCacheFilter.isContentHashedAssetPath(
                          "/assets/platforms/" + file.getFileName(), "", ""),
                      file.getFileName().toString()));
    }

    Path themes = repoPath("datahub-web-react/src/conf/theme");
    try (Stream<Path> files = Files.list(themes)) {
      files
          .filter(path -> path.getFileName().toString().endsWith(".json"))
          .forEach(
              file ->
                  assertFalse(
                      ContentHashedAssetCacheFilter.isContentHashedAssetPath(
                          "/assets/conf/theme/" + file.getFileName(), "", ""),
                      file.getFileName().toString()));
    }
  }

  private static void assertImmutable(String path) {
    assertImmutable(path, "", "");
  }

  private static void assertImmutable(String path, String httpContext, String basePath) {
    Result updated = cache(path, httpContext, basePath, javascript());
    assertEquals(
        ContentHashedAssetCacheFilter.IMMUTABLE_CACHE_CONTROL,
        updated.headers().get(Http.HeaderNames.CACHE_CONTROL),
        path);
  }

  private static void assertRevalidates(String path) {
    assertRevalidates(path, "", "");
  }

  private static void assertRevalidates(String path, String httpContext, String basePath) {
    Result updated = cache(path, httpContext, basePath, javascript());
    assertEquals(ONE_HOUR, updated.headers().get(Http.HeaderNames.CACHE_CONTROL), path);
  }

  private static void assertUnchanged(String path, Result original) {
    assertUnchanged(path, original, "", "");
  }

  private static void assertUnchanged(
      String path, Result original, String httpContext, String basePath) {
    Result updated = cache(path, httpContext, basePath, original);
    assertEquals(
        original.headers().get(Http.HeaderNames.CACHE_CONTROL),
        updated.headers().get(Http.HeaderNames.CACHE_CONTROL),
        path);
  }

  private static Result cache(String path, String httpContext, String basePath, Result original) {
    return ContentHashedAssetCacheFilter.withImmutableCacheIfHashed(
        path, httpContext, basePath, original);
  }

  private static Result javascript() {
    return Results.ok("payload")
        .as("application/javascript")
        .withHeader(Http.HeaderNames.CACHE_CONTROL, ONE_HOUR);
  }

  private static Path repoPath(String relative) {
    Path cursor = Path.of("").toAbsolutePath();
    for (int i = 0; i < 5 && cursor != null; i++) {
      Path candidate = cursor.resolve(relative);
      if (Files.exists(candidate)) {
        return candidate;
      }
      cursor = cursor.getParent();
    }
    throw new IllegalStateException("Could not find " + relative + " from the working directory");
  }
}
