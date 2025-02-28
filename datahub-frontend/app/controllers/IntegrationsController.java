package controllers;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.util.ByteString;
import com.datahub.authentication.AuthenticationConstants;
import com.linkedin.util.Pair;
import com.typesafe.config.Config;
import java.net.URI;
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
import play.libs.ws.InMemoryBodyWritable;
import play.libs.ws.StandaloneWSClient;
import play.libs.ws.ahc.StandaloneAhcWSClient;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.ResponseHeader;
import play.mvc.Result;
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClient;
import play.shaded.ahc.org.asynchttpclient.AsyncHttpClientConfig;
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClient;
import play.shaded.ahc.org.asynchttpclient.DefaultAsyncHttpClientConfig;
import utils.AcrylConstants;
import utils.ConfigUtil;

public class IntegrationsController extends Controller {

  private final Logger _logger = LoggerFactory.getLogger(Application.class.getName());
  private final Config _config;
  private final StandaloneWSClient _ws;

  @Inject
  public IntegrationsController(final @Nonnull Config config) {
    _config = config;
    _ws = createWsClient();
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

    if (headers.containsKey(Http.HeaderNames.HOST)
        && !headers.containsKey(Http.HeaderNames.X_FORWARDED_HOST)) {
      headers.put(Http.HeaderNames.X_FORWARDED_HOST, headers.get(Http.HeaderNames.HOST));
    }

    if (!headers.containsKey(Http.HeaderNames.X_FORWARDED_PROTO)) {
      final String schema =
          Optional.ofNullable(URI.create(request.uri()).getScheme()).orElse("http");
      headers.put(Http.HeaderNames.X_FORWARDED_PROTO, List.of(schema));
    }

    return _ws.url(
            String.format(
                "%s://%s:%s%s",
                protocol,
                integrationsServiceHost,
                integrationsServicePort,
                remapPath(request.uri())))
        .setMethod(request.method())
        .setHeaders(
            headers.entrySet().stream()
                // Remove X-DataHub-Actor to prevent malicious delegation.
                .filter(
                    entry ->
                        !AuthenticationConstants.LEGACY_X_DATAHUB_ACTOR_HEADER.equalsIgnoreCase(
                            entry.getKey()))
                .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
                .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
                // Remove Host s.th. service meshes do not route to wrong host
                .filter(entry -> !Http.HeaderNames.HOST.equals(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .setBody(
            new InMemoryBodyWritable(
                ByteString.fromByteBuffer(request.body().asBytes().asByteBuffer()),
                request.contentType().orElse("application/json")))
        .setRequestTimeout(Duration.ofSeconds(120))
        .execute()
        .thenApply(
            apiResponse -> {
              final ResponseHeader header =
                  new ResponseHeader(
                      apiResponse.getStatus(),
                      apiResponse.getHeaders().entrySet().stream()
                          .filter(entry -> !Http.HeaderNames.CONTENT_LENGTH.equals(entry.getKey()))
                          .filter(entry -> !Http.HeaderNames.CONTENT_TYPE.equals(entry.getKey()))
                          .map(entry -> Pair.of(entry.getKey(), String.join(";", entry.getValue())))
                          .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
              final HttpEntity body =
                  new HttpEntity.Strict(
                      apiResponse.getBodyAsBytes(),
                      Optional.ofNullable(apiResponse.getContentType()));
              return new Result(header, body);
            })
        .toCompletableFuture();
  }

  private StandaloneWSClient createWsClient() {
    final String name = "proxyClient";
    ActorSystem system = ActorSystem.create(name);
    system.registerOnTermination(() -> System.exit(0));
    Materializer materializer = ActorMaterializer.create(system);
    AsyncHttpClientConfig asyncHttpClientConfig =
        new DefaultAsyncHttpClientConfig.Builder()
            .setDisableUrlEncodingForBoundRequests(true)
            .setMaxRequestRetry(0)
            .setShutdownQuietPeriod(0)
            .setShutdownTimeout(0)
            .build();
    AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient(asyncHttpClientConfig);
    return new StandaloneAhcWSClient(asyncHttpClient, materializer);
  }

  private static String remapPath(@Nonnull final String original) {
    // Remap /integrations to /public. '
    // TODO: Make this configurable externally.
    return original.replaceFirst("/integrations", "/public");
  }
}
