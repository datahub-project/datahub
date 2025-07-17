package controllers;

import akka.util.ByteString;
import com.datahub.authentication.AuthenticationConstants;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.http.HttpEntity;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.ResponseHeader;
import play.mvc.Result;
import utils.AcrylConstants;
import utils.ConfigUtil;

public class IntegrationsController extends Controller {

  private final Logger _logger = LoggerFactory.getLogger(IntegrationsController.class.getName());
  private final Config _config;
  private final HttpClient _httpClient;

  @Inject
  public IntegrationsController(final @Nonnull Config config) {
    _config = config;
    _httpClient = createHttpClient();
  }

  /** Proxies requests to the Acryl Integrations Service */
  public CompletableFuture<Result> proxyToIntegrationsService(String path, Http.Request request)
      throws ExecutionException, InterruptedException {
    final String integrationsServiceHost =
        ConfigUtil.getString(
            _config,
            AcrylConstants.INTEGRATIONS_SERVICE_HOST_CONFIG_PATH,
            AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_HOST);
    final int integrationsServicePort =
        ConfigUtil.getInt(
            _config,
            AcrylConstants.INTEGRATIONS_SERVICE_PORT_CONFIG_PATH,
            AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_PORT);
    final boolean integrationsServiceUseSsl =
        ConfigUtil.getBoolean(
            _config,
            AcrylConstants.INTEGRATIONS_SERVICE_USE_SSL_CONFIG_PATH,
            AcrylConstants.DEFAULT_INTEGRATIONS_SERVICE_USE_SSL);

    final String protocol = integrationsServiceUseSsl ? "https" : "http";
    final Map<String, List<String>> headers = request.getHeaders().toMap();

    // Add forwarded headers
    if (headers.containsKey(Http.HeaderNames.HOST)
        && !headers.containsKey(Http.HeaderNames.X_FORWARDED_HOST)) {
      headers.put(Http.HeaderNames.X_FORWARDED_HOST, headers.get(Http.HeaderNames.HOST));
    }

    if (!headers.containsKey(Http.HeaderNames.X_FORWARDED_PROTO)) {
      final String schema =
          Optional.ofNullable(URI.create(request.uri()).getScheme()).orElse("http");
      headers.put(Http.HeaderNames.X_FORWARDED_PROTO, List.of(schema));
    }

    // Build the target URL
    String targetUrl =
        String.format(
            "%s://%s:%s%s",
            protocol, integrationsServiceHost, integrationsServicePort, remapPath(request.uri()));

    // Create HttpRequest.Builder
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder().uri(URI.create(targetUrl)).timeout(Duration.ofSeconds(120));

    // Set HTTP method with body if needed
    byte[] bodyBytes = request.body().asBytes().toArray();
    String contentType = request.contentType().orElse("application/json");

    switch (request.method()) {
      case "GET":
        requestBuilder.GET();
        break;
      case "POST":
        requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        requestBuilder.header(Http.HeaderNames.CONTENT_TYPE, contentType);
        break;
      case "PUT":
        requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        requestBuilder.header(Http.HeaderNames.CONTENT_TYPE, contentType);
        break;
      case "DELETE":
        requestBuilder.DELETE();
        break;
      case "PATCH":
        requestBuilder.method("PATCH", HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        requestBuilder.header(Http.HeaderNames.CONTENT_TYPE, contentType);
        break;
      default:
        requestBuilder.method(request.method(), HttpRequest.BodyPublishers.ofByteArray(bodyBytes));
        if (bodyBytes.length > 0) {
          requestBuilder.header(Http.HeaderNames.CONTENT_TYPE, contentType);
        }
    }

    // Add headers (filtering out unwanted ones)
    headers.entrySet().stream()
        // Remove X-DataHub-Actor to prevent malicious delegation
        .filter(
            entry ->
                !AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER.equalsIgnoreCase(
                    entry.getKey()))
        // Remove headers that will be set by HttpClient
        .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
        .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
        // Remove Host so service meshes don't route to wrong host
        .filter(entry -> !Http.HeaderNames.HOST.equals(entry.getKey()))
        .forEach(
            entry -> {
              // HttpClient expects individual header values, not a list
              entry.getValue().forEach(value -> requestBuilder.header(entry.getKey(), value));
            });

    HttpRequest httpRequest = requestBuilder.build();

    // Execute request asynchronously
    return _httpClient
        .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
        .thenApply(
            response -> {
              // Build Play response headers
              Map<String, String> responseHeaders =
                  response.headers().map().entrySet().stream()
                      .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
                      .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey, entry -> String.join(";", entry.getValue())));

              ResponseHeader header = new ResponseHeader(response.statusCode(), responseHeaders);

              // Get content type from response headers
              Optional<String> responseContentType =
                  response.headers().firstValue(Http.HeaderNames.CONTENT_TYPE);

              HttpEntity body =
                  new HttpEntity.Strict(ByteString.fromArray(response.body()), responseContentType);

              return new Result(header, body);
            })
        .exceptionally(
            throwable -> {
              _logger.error("Error proxying request to integrations service", throwable);
              // Return 502 Bad Gateway on proxy errors
              return internalServerError("Error communicating with integrations service");
            });
  }

  private HttpClient createHttpClient() {
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NEVER)
        .version(HttpClient.Version.HTTP_1_1)
        .build();
  }

  private static String remapPath(@Nonnull final String original) {
    // Remap /integrations to /public
    // TODO: Make this configurable externally.
    return original.replaceFirst("/integrations", "/public");
  }
}
