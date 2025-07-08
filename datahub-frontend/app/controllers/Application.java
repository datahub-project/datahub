package controllers;

import static auth.AuthUtils.ACTOR;
import static auth.AuthUtils.SESSION_COOKIE_GMS_TOKEN_NAME;

import akka.util.ByteString;
import auth.Authenticator;
import com.datahub.authentication.AuthenticationConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.util.Pair;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.Environment;
import play.http.HttpEntity;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Http.Cookie;
import play.mvc.ResponseHeader;
import play.mvc.Result;
import play.mvc.Security;
import utils.ConfigUtil;

public class Application extends Controller {
  private static final Set<String> RESTRICTED_HEADERS =
      Set.of("connection", "host", "content-length", "expect", "upgrade", "transfer-encoding");

  private final Logger _logger = LoggerFactory.getLogger(Application.class.getName());
  private final Config _config;
  private final Environment _environment;
  private final HttpClient httpClient =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @Inject
  public Application(Environment environment, @Nonnull Config config) {
    _config = config;
    _environment = environment;
  }

  /** Serves the build output index.html for any given path */
  @Nonnull
  private Result serveAsset(@Nullable String path) {
    try {
      InputStream indexHtml = _environment.resourceAsStream("public/index.html");
      return ok(indexHtml).withHeader("Cache-Control", "no-cache").as("text/html");
    } catch (Exception e) {
      _logger.warn("Cannot load public/index.html resource. Static assets or assets jar missing?");
      return notFound().withHeader("Cache-Control", "no-cache").as("text/html");
    }
  }

  @Nonnull
  public Result healthcheck() {
    return ok("GOOD");
  }

  /** index Action proxies to serveAsset */
  @Nonnull
  public Result index(@Nullable String path) {
    return serveAsset("");
  }

  /** Proxies requests to the Metadata Service using Java HttpClient. */
  @Security.Authenticated(Authenticator.class)
  public CompletableFuture<Result> proxy(String path, Http.Request request) {
    final String authorizationHeaderValue = getAuthorizationHeaderValueToProxy(request);
    final String resolvedUri = mapPath(request.uri());

    final String metadataServiceHost =
        ConfigUtil.getString(
            _config,
            ConfigUtil.METADATA_SERVICE_HOST_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_HOST);
    final int metadataServicePort =
        ConfigUtil.getInt(
            _config,
            ConfigUtil.METADATA_SERVICE_PORT_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_PORT);
    final boolean metadataServiceUseSsl =
        ConfigUtil.getBoolean(
            _config,
            ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL);

    final String protocol = metadataServiceUseSsl ? "https" : "http";
    final String targetUrl =
        String.format(
            "%s://%s:%s%s", protocol, metadataServiceHost, metadataServicePort, resolvedUri);

    // Prepare request builder
    HttpRequest.Builder httpRequestBuilder =
        HttpRequest.newBuilder().uri(URI.create(targetUrl)).timeout(Duration.ofSeconds(120));

    // Set HTTP method and body
    httpRequestBuilder.method(request.method(), buildBodyPublisher(request));

    // Set headers, filtering restricted and application-specific ones
    Map<String, List<String>> headers = request.getHeaders().toMap();
    if (headers.containsKey(Http.HeaderNames.HOST)
        && !headers.containsKey(Http.HeaderNames.X_FORWARDED_HOST)) {
      headers.put(Http.HeaderNames.X_FORWARDED_HOST, headers.get(Http.HeaderNames.HOST));
    }
    if (!headers.containsKey(Http.HeaderNames.X_FORWARDED_PROTO)) {
      final String schema =
          Optional.ofNullable(URI.create(request.uri()).getScheme()).orElse("http");
      headers.put(Http.HeaderNames.X_FORWARDED_PROTO, List.of(schema));
    }

    // Copy headers except restricted and a few special ones
    headers.entrySet().stream()
        .filter(
            entry ->
                !RESTRICTED_HEADERS.contains(entry.getKey().toLowerCase())
                    && !AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER.equalsIgnoreCase(
                        entry.getKey())
                    && !Http.HeaderNames.CONTENT_TYPE.equalsIgnoreCase(entry.getKey())
                    && !Http.HeaderNames.AUTHORIZATION.equalsIgnoreCase(entry.getKey()))
        .forEach(
            entry -> entry.getValue().forEach(v -> httpRequestBuilder.header(entry.getKey(), v)));

    // Add Authorization header
    if (!authorizationHeaderValue.isEmpty()) {
      httpRequestBuilder.header(Http.HeaderNames.AUTHORIZATION, authorizationHeaderValue);
    }
    httpRequestBuilder.header(
        AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER, getDataHubActorHeader(request));
    // Set content type if present
    request
        .contentType()
        .ifPresent(ct -> httpRequestBuilder.header(Http.HeaderNames.CONTENT_TYPE, ct));

    Instant start = Instant.now();

    // Send the request asynchronously
    return httpClient
        .sendAsync(httpRequestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(
            apiResponse -> {
              boolean verboseGraphQLLogging = _config.getBoolean("graphql.verbose.logging");
              int verboseGraphQLLongQueryMillis = _config.getInt("graphql.verbose.slowQueryMillis");
              Instant finish = Instant.now();
              long timeElapsed = Duration.between(start, finish).toMillis();
              if (verboseGraphQLLogging && timeElapsed >= verboseGraphQLLongQueryMillis) {
                logSlowQuery(request, resolvedUri, timeElapsed);
              }
              // Build Play response
              ResponseHeader header =
                  new ResponseHeader(
                      apiResponse.statusCode(),
                      apiResponse.headers().map().entrySet().stream()
                          .filter(
                              entry ->
                                  !Http.HeaderNames.CONTENT_LENGTH.equalsIgnoreCase(entry.getKey()))
                          .filter(
                              entry ->
                                  !Http.HeaderNames.CONTENT_TYPE.equalsIgnoreCase(entry.getKey()))
                          .map(entry -> Pair.of(entry.getKey(), String.join(";", entry.getValue())))
                          .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
              HttpEntity body =
                  new HttpEntity.Strict(
                      ByteString.fromArray(apiResponse.body()),
                      apiResponse.headers().firstValue(Http.HeaderNames.CONTENT_TYPE));
              return new Result(header, body);
            });
  }

  // Helper to build the body publisher from the Play request
  private HttpRequest.BodyPublisher buildBodyPublisher(Http.Request request) {
    if (request.body().asBytes() != null) {
      return HttpRequest.BodyPublishers.ofByteArray(request.body().asBytes().toArray());
    } else if (request.body().asText() != null) {
      return HttpRequest.BodyPublishers.ofString(request.body().asText());
    }
    return HttpRequest.BodyPublishers.noBody();
  }

  /** Creates a wrapping ObjectNode containing config information */
  @Nonnull
  public Result appConfig() {
    final ObjectNode config = Json.newObject();
    config.put("application", "datahub-frontend");
    config.put("appVersion", _config.getString("app.version"));
    config.put("isInternal", _config.getBoolean("linkedin.internal"));
    config.put("shouldShowDatasetLineage", _config.getBoolean("linkedin.show.dataset.lineage"));
    config.put(
        "suggestionConfidenceThreshold",
        Integer.valueOf(_config.getString("linkedin.suggestion.confidence.threshold")));
    config.set("wikiLinks", wikiLinks());
    config.set("tracking", trackingInfo());
    config.put("isStagingBanner", _config.getBoolean("ui.show.staging.banner"));
    config.put("isLiveDataWarning", _config.getBoolean("ui.show.live.data.banner"));
    config.put("showChangeManagement", _config.getBoolean("ui.show.CM.banner"));
    config.put("showPeople", _config.getBoolean("ui.show.people"));
    config.put("changeManagementLink", _config.getString("ui.show.CM.link"));
    config.put("isStaleSearch", _config.getBoolean("ui.show.stale.search"));
    config.put("showAdvancedSearch", _config.getBoolean("ui.show.advanced.search"));
    config.put("useNewBrowseDataset", _config.getBoolean("ui.new.browse.dataset"));
    config.put("showLineageGraph", _config.getBoolean("ui.show.lineage.graph"));
    config.put("showInstitutionalMemory", _config.getBoolean("ui.show.institutional.memory"));
    config.set("userEntityProps", userEntityProps());

    final ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("config", config);
    return ok(response);
  }

  /** Creates a JSON object of profile / avatar properties */
  @Nonnull
  private ObjectNode userEntityProps() {
    final ObjectNode props = Json.newObject();
    props.put("aviUrlPrimary", _config.getString("linkedin.links.avi.urlPrimary"));
    props.put("aviUrlFallback", _config.getString("linkedin.links.avi.urlFallback"));
    return props;
  }

  /**
   * @return Json object with internal wiki links
   */
  @Nonnull
  private ObjectNode wikiLinks() {
    final ObjectNode wikiLinks = Json.newObject();
    wikiLinks.put("appHelp", _config.getString("links.wiki.appHelp"));
    wikiLinks.put("gdprPii", _config.getString("links.wiki.gdprPii"));
    wikiLinks.put("tmsSchema", _config.getString("links.wiki.tmsSchema"));
    wikiLinks.put("gdprTaxonomy", _config.getString("links.wiki.gdprTaxonomy"));
    wikiLinks.put("staleSearchIndex", _config.getString("links.wiki.staleSearchIndex"));
    wikiLinks.put("dht", _config.getString("links.wiki.dht"));
    wikiLinks.put("purgePolicies", _config.getString("links.wiki.purgePolicies"));
    wikiLinks.put("jitAcl", _config.getString("links.wiki.jitAcl"));
    wikiLinks.put("metadataCustomRegex", _config.getString("links.wiki.metadataCustomRegex"));
    wikiLinks.put("exportPolicy", _config.getString("links.wiki.exportPolicy"));
    wikiLinks.put("metadataHealth", _config.getString("links.wiki.metadataHealth"));
    wikiLinks.put("purgeKey", _config.getString("links.wiki.purgeKey"));
    wikiLinks.put("datasetDecommission", _config.getString("links.wiki.datasetDecommission"));
    return wikiLinks;
  }

  /**
   * @return Json object containing the tracking configuration details
   */
  @Nonnull
  private ObjectNode trackingInfo() {
    final ObjectNode piwik = Json.newObject();
    piwik.put("piwikSiteId", Integer.valueOf(_config.getString("tracking.piwik.siteid")));
    piwik.put("piwikUrl", _config.getString("tracking.piwik.url"));

    final ObjectNode trackers = Json.newObject();
    trackers.set("piwik", piwik);

    final ObjectNode trackingConfig = Json.newObject();
    trackingConfig.set("trackers", trackers);
    trackingConfig.put("isEnabled", true);
    return trackingConfig;
  }

  /**
   * Returns the value of the Authorization Header to be provided when proxying requests to the
   * downstream Metadata Service.
   */
  private String getAuthorizationHeaderValueToProxy(Http.Request request) {
    String value = "";
    if (request.session().data().containsKey(SESSION_COOKIE_GMS_TOKEN_NAME)) {
      value = "Bearer " + request.session().data().get(SESSION_COOKIE_GMS_TOKEN_NAME);
    } else if (request.getHeaders().contains(Http.HeaderNames.AUTHORIZATION)) {
      value = request.getHeaders().get(Http.HeaderNames.AUTHORIZATION).get();
    }
    return value;
  }

  /** Returns the value of the legacy X-DataHub-Actor header to forward to the Metadata Service. */
  private String getDataHubActorHeader(Http.Request request) {
    String actor = request.session().data().get(ACTOR);
    return actor == null ? "" : actor;
  }

  private String mapPath(@Nonnull final String path) {
    if (path.equals("/api/v2/graphql")) {
      return "/api/graphql";
    }
    final String gmsApiPath = "/api/gms";
    if (path.startsWith(gmsApiPath)) {
      String newPath = path.substring(gmsApiPath.length());
      if (!newPath.startsWith("/")) {
        newPath = "/" + newPath;
      }
      return newPath;
    }
    return path;
  }

  /**
   * Called if verbose logging is enabled and request takes longer that the slow query milliseconds
   * defined in the config
   */
  private void logSlowQuery(Http.Request request, String resolvedUri, float duration) {
    StringBuilder jsonBody = new StringBuilder();
    Optional<Cookie> actorCookie = request.getCookie("actor");
    String actorValue = actorCookie.isPresent() ? actorCookie.get().value() : "N/A";

    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode = request.body().asJson();
      if (jsonNode != null && jsonNode.isObject()) {
        ((ObjectNode) jsonNode).remove("query");
        jsonBody.append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode));
      }
    } catch (Exception e) {
      _logger.info("GraphQL Request Received: {}, Unable to parse JSON body", resolvedUri);
    }
    String jsonBodyStr = jsonBody.toString();
    _logger.info(
        "Slow GraphQL Request Received: {}, Request query string: {}, Request actor: {}, Request JSON: {}, Request completed in {} ms",
        resolvedUri,
        request.queryString(),
        actorValue,
        jsonBodyStr,
        duration);
  }
}
