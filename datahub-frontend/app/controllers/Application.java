package controllers;

import static auth.AuthUtils.ACTOR;
import static auth.AuthUtils.SESSION_COOKIE_GMS_TOKEN_NAME;

import akka.util.ByteString;
import auth.Authenticator;
import com.datahub.authentication.AuthenticationConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.utils.BasePathUtils;
import com.linkedin.util.Pair;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.List;
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
  private static final Logger logger = LoggerFactory.getLogger(Application.class.getName());
  private static final Set<String> RESTRICTED_HEADERS =
      Set.of("connection", "host", "content-length", "expect", "upgrade", "transfer-encoding");
  private static final Set<String> SWAGGER_PATHS =
      Set.of("/openapi/swagger-ui", "/openapi/v3/api-docs");
  private final HttpClient httpClient;

  private final Config config;
  private final Environment environment;

  private final String basePath;
  private final String gaTrackingId;
  private final List<String> streamingPathPrefixes;

  @Inject
  public Application(HttpClient httpClient, Environment environment, @Nonnull Config config) {
    this.httpClient = httpClient;
    this.config = config;
    this.environment = environment;
    this.basePath = config.getString("datahub.basePath");
    this.gaTrackingId =
        config.hasPath("analytics.google.tracking.id")
            ? config.getString("analytics.google.tracking.id")
            : null;
    this.streamingPathPrefixes = resolveStreamingPathPrefixes(config);
  }

  static List<String> resolveStreamingPathPrefixes(Config config) {
    if (config.hasPath("proxy.streamingPathPrefixes")) {
      String value = config.getString("proxy.streamingPathPrefixes");
      if (value != null && !value.isBlank()) {
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
      }
    }
    return List.of();
  }

  /**
   * Serves the build output index.html for any given path
   *
   * @param path takes a path string, which essentially is ignored routing is managed client side
   * @return {Result} rendered index template with dynamic base path
   */
  @Nonnull
  private Result serveAsset(@Nullable String path) {
    try {
      InputStream indexHtml = environment.resourceAsStream("public/index.html");
      if (indexHtml == null) {
        throw new IllegalStateException("index.html not found");
      }

      String html = new String(indexHtml.readAllBytes(), StandardCharsets.UTF_8);

      String basePath = this.basePath;
      // Ensure base path ends with / for HTML base tag
      if (!basePath.endsWith("/")) {
        basePath += "/";
      }
      // Inject <base href="..."/> right after <head> for use in the frontend.
      String modifiedHtml = html.replace("@basePath", basePath);

      // Inject google tracking if it exists, should only be enabled for demo site
      if (gaTrackingId != null && !gaTrackingId.isEmpty()) {
        String gaScript =
            String.format(
                "<script async src=\"https://www.googletagmanager.com/gtag/js?id=%s\"></script>"
                    + "<script>window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments);}"
                    + "gtag('js',new Date());gtag('config','%s');</script>",
                gaTrackingId, gaTrackingId);
        modifiedHtml = modifiedHtml.replace("</head>", gaScript + "</head>");
      }

      return ok(modifiedHtml).withHeader("Cache-Control", "no-cache").as("text/html");
    } catch (Exception e) {
      logger.warn("Cannot load public/index.html resource. Static assets or assets jar missing?");
      return notFound().withHeader("Cache-Control", "no-cache").as("text/html");
    }
  }

  @Nonnull
  public Result healthcheck() {
    return ok("GOOD");
  }

  /**
   * index Action proxies to serveAsset
   *
   * @return {Result} response from serveAsset method
   */
  @Nonnull
  public Result index(@Nullable String path) {
    return serveAsset(path);
  }

  /**
   * Proxies requests to the Metadata Service
   *
   * <p>TODO: Investigate using mutual SSL authentication to call Metadata Service.
   */
  @Security.Authenticated(Authenticator.class)
  public CompletableFuture<Result> proxy(String path, Http.Request request) {
    final String authorizationHeaderValue = getAuthorizationHeaderValueToProxy(request);
    final String resolvedUri = mapPath(request.uri());

    final String metadataServiceHost =
        ConfigUtil.getString(
            config,
            ConfigUtil.METADATA_SERVICE_HOST_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_HOST);
    final int metadataServicePort =
        ConfigUtil.getInt(
            config,
            ConfigUtil.METADATA_SERVICE_PORT_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_PORT);
    final String metadataServiceBasePath =
        ConfigUtil.getString(
            config,
            ConfigUtil.METADATA_SERVICE_BASE_PATH_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_BASE_PATH);
    final boolean metadataServiceBasePathEnabled =
        ConfigUtil.getBoolean(
            config,
            ConfigUtil.METADATA_SERVICE_BASE_PATH_ENABLED_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_BASE_PATH_ENABLED);
    final boolean metadataServiceUseSsl =
        ConfigUtil.getBoolean(
            config,
            ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL);

    // Use the same logic as GMSConfiguration.getResolvedBasePath()
    String resolvedBasePath =
        BasePathUtils.resolveBasePath(metadataServiceBasePathEnabled, metadataServiceBasePath);

    final String protocol = metadataServiceUseSsl ? "https" : "http";
    final String targetUrl =
        String.format(
            "%s://%s:%s%s%s",
            protocol, metadataServiceHost, metadataServicePort, resolvedBasePath, resolvedUri);
    HttpRequest.Builder httpRequestBuilder =
        HttpRequest.newBuilder().uri(URI.create(targetUrl)).timeout(Duration.ofSeconds(120));
    httpRequestBuilder.method(request.method(), buildBodyPublisher(request));
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
    if (!authorizationHeaderValue.isEmpty()) {
      httpRequestBuilder.header(Http.HeaderNames.AUTHORIZATION, authorizationHeaderValue);
    }
    httpRequestBuilder.header(
        AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER, getDataHubActorHeader(request));
    request
        .contentType()
        .ifPresent(ct -> httpRequestBuilder.header(Http.HeaderNames.CONTENT_TYPE, ct));
    Instant start = Instant.now();
    boolean useStreaming =
        streamingPathPrefixes.stream().anyMatch(prefix -> resolvedUri.startsWith(prefix));

    HttpResponse.BodyHandler<?> bodyHandler =
        useStreaming
            ? HttpResponse.BodyHandlers.ofInputStream()
            : HttpResponse.BodyHandlers.ofByteArray();

    return httpClient
        .sendAsync(httpRequestBuilder.build(), bodyHandler)
        .thenApply(
            apiResponse -> buildProxyResult(request, resolvedUri, start, apiResponse, useStreaming))
        .exceptionally(this::handleProxyException);
  }

  private Result buildProxyResult(
      Http.Request request,
      String resolvedUri,
      Instant start,
      HttpResponse<?> apiResponse,
      boolean useStreaming) {
    boolean verboseGraphQLLogging = config.getBoolean("graphql.verbose.logging");
    int verboseGraphQLLongQueryMillis = config.getInt("graphql.verbose.slowQueryMillis");
    long timeElapsed = Duration.between(start, Instant.now()).toMillis();
    if (verboseGraphQLLogging && timeElapsed >= verboseGraphQLLongQueryMillis) {
      logSlowQuery(request, resolvedUri, (float) timeElapsed);
    }
    ResponseHeader header = buildProxyResponseHeader(apiResponse, useStreaming);
    Optional<String> contentType = apiResponse.headers().firstValue(Http.HeaderNames.CONTENT_TYPE);
    HttpEntity body =
        useStreaming
            ? new HttpEntity.Streamed(
                akka.stream.javadsl.StreamConverters.fromInputStream(
                    () -> (InputStream) apiResponse.body()),
                Optional.empty(),
                contentType)
            : new HttpEntity.Strict(ByteString.fromArray((byte[]) apiResponse.body()), contentType);
    return new Result(header, body);
  }

  private ResponseHeader buildProxyResponseHeader(
      HttpResponse<?> apiResponse, boolean useStreaming) {
    Map<String, String> headers =
        apiResponse.headers().map().entrySet().stream()
            .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equalsIgnoreCase(entry.getKey()))
            .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equalsIgnoreCase(entry.getKey()))
            .filter(
                entry ->
                    !Http.HeaderNames.CONTENT_ENCODING.equalsIgnoreCase(entry.getKey())
                        && !Http.HeaderNames.TRANSFER_ENCODING.equalsIgnoreCase(entry.getKey()))
            .map(entry -> Pair.of(entry.getKey(), String.join(";", entry.getValue())))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    if (useStreaming) {
      headers.put(Http.HeaderNames.CONTENT_ENCODING, "identity");
    }
    return new ResponseHeader(apiResponse.statusCode(), headers);
  }

  private Result handleProxyException(Throwable ex) {
    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
    if (cause instanceof java.net.http.HttpTimeoutException) {
      return status(GATEWAY_TIMEOUT, "Proxy request timed out.");
    } else if (cause instanceof java.net.ConnectException) {
      return status(BAD_GATEWAY, "Proxy connection failed: " + cause.getMessage());
    } else {
      return internalServerError("Proxy error: " + cause.getMessage());
    }
  }

  private HttpRequest.BodyPublisher buildBodyPublisher(Http.Request request) {
    if (request.body().asBytes() != null) {
      return HttpRequest.BodyPublishers.ofByteArray(request.body().asBytes().toArray());
    } else if (request.body().asText() != null) {
      return HttpRequest.BodyPublishers.ofString(request.body().asText());
    }
    return HttpRequest.BodyPublishers.noBody();
  }

  /**
   * Creates a wrapping ObjectNode containing config information
   *
   * @return Http Result instance with app configuration attributes
   */
  @Nonnull
  public Result appConfig() {
    final ObjectNode config = Json.newObject();
    config.put("application", "datahub-frontend");
    config.put("appVersion", this.config.getString("app.version"));
    config.put("isInternal", this.config.getBoolean("linkedin.internal"));
    config.put("shouldShowDatasetLineage", this.config.getBoolean("linkedin.show.dataset.lineage"));
    config.put(
        "suggestionConfidenceThreshold",
        Integer.valueOf(this.config.getString("linkedin.suggestion.confidence.threshold")));
    config.set("wikiLinks", wikiLinks());
    config.set("tracking", trackingInfo());
    // In a staging environment, we can trigger this flag to be true so that the UI can handle based
    // on
    // such config and alert users that their changes will not affect production data
    config.put("isStagingBanner", this.config.getBoolean("ui.show.staging.banner"));
    config.put("isLiveDataWarning", this.config.getBoolean("ui.show.live.data.banner"));
    config.put("showChangeManagement", this.config.getBoolean("ui.show.CM.banner"));
    // Flag to enable people entity elements
    config.put("showPeople", this.config.getBoolean("ui.show.people"));
    config.put("changeManagementLink", this.config.getString("ui.show.CM.link"));
    // Flag set in order to warn users that search is experiencing issues
    config.put("isStaleSearch", this.config.getBoolean("ui.show.stale.search"));
    config.put("showAdvancedSearch", this.config.getBoolean("ui.show.advanced.search"));
    // Flag to use the new api for browsing datasets
    config.put("useNewBrowseDataset", this.config.getBoolean("ui.new.browse.dataset"));
    // show lineage graph in relationships tabs
    config.put("showLineageGraph", this.config.getBoolean("ui.show.lineage.graph"));
    // show institutional memory for available entities
    config.put("showInstitutionalMemory", this.config.getBoolean("ui.show.institutional.memory"));

    // Insert properties for user profile operations
    config.set("userEntityProps", userEntityProps());

    // Add base path configuration for frontend
    config.put("basePath", this.basePath);

    final ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("config", config);
    return ok(response);
  }

  /**
   * Creates a JSON object of profile / avatar properties
   *
   * @return Json avatar / profile image properties
   */
  @Nonnull
  private ObjectNode userEntityProps() {
    final ObjectNode props = Json.newObject();
    props.put("aviUrlPrimary", config.getString("linkedin.links.avi.urlPrimary"));
    props.put("aviUrlFallback", config.getString("linkedin.links.avi.urlFallback"));
    return props;
  }

  /**
   * @return Json object with internal wiki links
   */
  @Nonnull
  private ObjectNode wikiLinks() {
    final ObjectNode wikiLinks = Json.newObject();
    wikiLinks.put("appHelp", config.getString("links.wiki.appHelp"));
    wikiLinks.put("gdprPii", config.getString("links.wiki.gdprPii"));
    wikiLinks.put("tmsSchema", config.getString("links.wiki.tmsSchema"));
    wikiLinks.put("gdprTaxonomy", config.getString("links.wiki.gdprTaxonomy"));
    wikiLinks.put("staleSearchIndex", config.getString("links.wiki.staleSearchIndex"));
    wikiLinks.put("dht", config.getString("links.wiki.dht"));
    wikiLinks.put("purgePolicies", config.getString("links.wiki.purgePolicies"));
    wikiLinks.put("jitAcl", config.getString("links.wiki.jitAcl"));
    wikiLinks.put("metadataCustomRegex", config.getString("links.wiki.metadataCustomRegex"));
    wikiLinks.put("exportPolicy", config.getString("links.wiki.exportPolicy"));
    wikiLinks.put("metadataHealth", config.getString("links.wiki.metadataHealth"));
    wikiLinks.put("purgeKey", config.getString("links.wiki.purgeKey"));
    wikiLinks.put("datasetDecommission", config.getString("links.wiki.datasetDecommission"));
    return wikiLinks;
  }

  /**
   * @return Json object containing the tracking configuration details
   */
  @Nonnull
  private ObjectNode trackingInfo() {
    final ObjectNode piwik = Json.newObject();
    piwik.put("piwikSiteId", Integer.valueOf(config.getString("tracking.piwik.siteid")));
    piwik.put("piwikUrl", config.getString("tracking.piwik.url"));

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
   *
   * <p>Currently, the Authorization header value may be derived from
   *
   * <p>a) The value of the "token" attribute of the Session Cookie provided by the client. This
   * value is set when creating the session token initially from a token granted by the Metadata
   * Service.
   *
   * <p>Or if the "token" attribute cannot be found in a session cookie, then we fallback to
   *
   * <p>b) The value of the Authorization header provided in the original request. This will be used
   * in cases where clients are making programmatic requests to Metadata Service APIs directly,
   * without providing a session cookie (ui only).
   *
   * <p>If neither are found, an empty string is returned.
   */
  private String getAuthorizationHeaderValueToProxy(Http.Request request) {
    // If the session cookie has an authorization token, use that. If there's an authorization
    // header provided, simply
    // use that.
    String value = "";
    if (request.session().data().containsKey(SESSION_COOKIE_GMS_TOKEN_NAME)) {
      value = "Bearer " + request.session().data().get(SESSION_COOKIE_GMS_TOKEN_NAME);
    } else if (request.getHeaders().contains(Http.HeaderNames.AUTHORIZATION)) {
      value = request.getHeaders().get(Http.HeaderNames.AUTHORIZATION).get();
    }
    return value;
  }

  /**
   * Returns the value of the legacy X-DataHub-Actor header to forward to the Metadata Service. This
   * is sent along with any requests that have a valid frontend session cookie to identify the
   * calling actor, for backwards compatibility.
   *
   * <p>If Metadata Service authentication is enabled, this value is not required because Actor
   * context will most often come from the authentication credentials provided in the Authorization
   * header.
   */
  private String getDataHubActorHeader(Http.Request request) {
    String actor = request.session().data().get(ACTOR);
    return actor == null ? "" : actor;
  }

  /**
   * Maps the request path to the backend path: strips datahub.basePath (when present) and applies
   * path rewrites (e.g. /api/v2/graphql → /api/graphql, /api/gms → /). All conditionals (GraphQL,
   * GMS, streaming) use this stripped path. When using a base path, set datahub.basePath to the
   * same value as play.http.context so that stripping works (Play may already strip context from
   * request.uri(); stripBasePath returns the path unchanged if the prefix is not present).
   */
  private String mapPath(@Nonnull final String path) {

    final String strippedPath;

    // Cannot strip base path from swagger urls
    if (SWAGGER_PATHS.stream().noneMatch(path::contains)) {
      strippedPath = BasePathUtils.stripBasePath(path, this.basePath);
    } else {
      strippedPath = path;
    }

    // Case 1: Map legacy GraphQL path to GMS GraphQL API (for compatibility)
    if (strippedPath.equals("/api/v2/graphql")) {
      return "/api/graphql";
    }

    // Case 2: Map requests to /gms to / (Rest.li API)
    final String gmsApiPath = "/api/gms";
    if (strippedPath.startsWith(gmsApiPath)) {
      String newPath = strippedPath.substring(gmsApiPath.length());
      if (!newPath.startsWith("/")) {
        newPath = "/" + newPath;
      }
      return newPath;
    }

    // Otherwise, return the stripped path
    return strippedPath;
  }

  /**
   * Called if verbose logging is enabled and request takes longer that the slow query milliseconds
   * defined in the config
   *
   * @param request GraphQL request that was made
   * @param resolvedUri URI that was requested
   * @param duration How long the query took to complete
   */
  private void logSlowQuery(Http.Request request, String resolvedUri, float duration) {
    StringBuilder jsonBody = new StringBuilder();
    Optional<Cookie> actorCookie = request.getCookie("actor");
    String actorValue = actorCookie.isPresent() ? actorCookie.get().value() : "N/A";

    // Get the JSON body
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode = request.body().asJson();
      ((ObjectNode) jsonNode).remove("query");
      jsonBody.append(mapper.writeValueAsString(jsonNode));
    } catch (Exception e) {
      logger.info("GraphQL Request Received: {}, Unable to parse JSON body", resolvedUri);
    }
    String jsonBodyStr = jsonBody.toString();

    // Get the query string
    StringBuilder query = new StringBuilder();
    try {
      ObjectMapper mapper = new ObjectMapper();
      query.append(mapper.writeValueAsString(request.queryString()));
    } catch (Exception e) {
      logger.info("GraphQL Request Received: {}, Unable to parse query string", resolvedUri);
    }
    String queryString = query.toString();

    logger.info(
        "Slow GraphQL Request Received: {}, Request query string: {}, Request actor: {}, Request JSON: {}, Request completed in {} ms",
        resolvedUri,
        queryString,
        actorValue,
        jsonBodyStr,
        duration);
  }
}
