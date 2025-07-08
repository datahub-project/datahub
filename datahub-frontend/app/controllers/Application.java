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
  private static final Logger logger = LoggerFactory.getLogger(Application.class.getName());
  private static final Set<String> RESTRICTED_HEADERS =
      Set.of("connection", "host", "content-length", "expect", "upgrade", "transfer-encoding");
  private final Config config;
  private final Environment environment;
  private final HttpClient httpClient =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @Inject
  public Application(Environment environment, @Nonnull Config config) {
    this.config = config;
    this.environment = environment;
  }

  @Nonnull
  private Result serveAsset(@Nullable String path) {
    try {
      InputStream indexHtml = environment.resourceAsStream("public/index.html");
      return ok(indexHtml).withHeader("Cache-Control", "no-cache").as("text/html");
    } catch (Exception e) {
      logger.warn("Cannot load public/index.html resource. Static assets or assets jar missing?");
      return notFound().withHeader("Cache-Control", "no-cache").as("text/html");
    }
  }

  @Nonnull
  public Result healthcheck() {
    return ok("GOOD");
  }

  @Nonnull
  public Result index(@Nullable String path) {
    return serveAsset("");
  }

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
    final boolean metadataServiceUseSsl =
        ConfigUtil.getBoolean(
            config,
            ConfigUtil.METADATA_SERVICE_USE_SSL_CONFIG_PATH,
            ConfigUtil.DEFAULT_METADATA_SERVICE_USE_SSL);

    final String protocol = metadataServiceUseSsl ? "https" : "http";
    final String targetUrl =
        String.format(
            "%s://%s:%s%s", protocol, metadataServiceHost, metadataServicePort, resolvedUri);

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

    return httpClient
        .sendAsync(httpRequestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(
            apiResponse -> {
              boolean verboseGraphQLLogging = config.getBoolean("graphql.verbose.logging");
              int verboseGraphQLLongQueryMillis = config.getInt("graphql.verbose.slowQueryMillis");
              Instant finish = Instant.now();
              long timeElapsed = Duration.between(start, finish).toMillis();
              if (verboseGraphQLLogging && timeElapsed >= verboseGraphQLLongQueryMillis) {
                logSlowQuery(request, resolvedUri, timeElapsed);
              }
              final ResponseHeader header =
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
              final HttpEntity body =
                  new HttpEntity.Strict(
                      ByteString.fromArray(apiResponse.body()),
                      apiResponse.headers().firstValue(Http.HeaderNames.CONTENT_TYPE));
              return new Result(header, body);
            })
        .exceptionally(
            ex -> {
              // Snap out of it on any error or timeout
              Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
              if (cause instanceof java.net.http.HttpTimeoutException) {
                return status(GATEWAY_TIMEOUT, "Proxy request timed out.");
              } else if (cause instanceof java.net.ConnectException) {
                return status(BAD_GATEWAY, "Proxy connection failed: " + cause.getMessage());
              } else {
                return internalServerError("Proxy error: " + cause.getMessage());
              }
            });
  }

  private HttpRequest.BodyPublisher buildBodyPublisher(Http.Request request) {
    if (request.body().asBytes() != null) {
      return HttpRequest.BodyPublishers.ofByteArray(request.body().asBytes().toArray());
    } else if (request.body().asText() != null) {
      return HttpRequest.BodyPublishers.ofString(request.body().asText());
    }
    return HttpRequest.BodyPublishers.noBody();
  }

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
    config.put("isStagingBanner", this.config.getBoolean("ui.show.staging.banner"));
    config.put("isLiveDataWarning", this.config.getBoolean("ui.show.live.data.banner"));
    config.put("showChangeManagement", this.config.getBoolean("ui.show.CM.banner"));
    config.put("showPeople", this.config.getBoolean("ui.show.people"));
    config.put("changeManagementLink", this.config.getString("ui.show.CM.link"));
    config.put("isStaleSearch", this.config.getBoolean("ui.show.stale.search"));
    config.put("showAdvancedSearch", this.config.getBoolean("ui.show.advanced.search"));
    config.put("useNewBrowseDataset", this.config.getBoolean("ui.new.browse.dataset"));
    config.put("showLineageGraph", this.config.getBoolean("ui.show.lineage.graph"));
    config.put("showInstitutionalMemory", this.config.getBoolean("ui.show.institutional.memory"));
    config.set("userEntityProps", userEntityProps());

    final ObjectNode response = Json.newObject();
    response.put("status", "ok");
    response.set("config", config);
    return ok(response);
  }

  @Nonnull
  private ObjectNode userEntityProps() {
    final ObjectNode props = Json.newObject();
    props.put("aviUrlPrimary", config.getString("linkedin.links.avi.urlPrimary"));
    props.put("aviUrlFallback", config.getString("linkedin.links.avi.urlFallback"));
    return props;
  }

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

  private String getAuthorizationHeaderValueToProxy(Http.Request request) {
    String value = "";
    if (request.session().data().containsKey(SESSION_COOKIE_GMS_TOKEN_NAME)) {
      value = "Bearer " + request.session().data().get(SESSION_COOKIE_GMS_TOKEN_NAME);
    } else if (request.getHeaders().contains(Http.HeaderNames.AUTHORIZATION)) {
      value = request.getHeaders().get(Http.HeaderNames.AUTHORIZATION).get();
    }
    return value;
  }

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
      logger.info("GraphQL Request Received: {}, Unable to parse JSON body", resolvedUri);
    }
    String jsonBodyStr = jsonBody.toString();
    logger.info(
        "Slow GraphQL Request Received: {}, Request query string: {}, Request actor: {}, Request JSON: {}, Request completed in {} ms",
        resolvedUri,
        request.queryString(),
        actorValue,
        jsonBodyStr,
        duration);
  }
}
