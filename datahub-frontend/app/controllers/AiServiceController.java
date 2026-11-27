package controllers;

import com.typesafe.config.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import org.apache.pekko.stream.javadsl.JavaFlowSupport;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.http.HttpEntity;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.ResponseHeader;
import play.mvc.Result;
import play.mvc.Results;

/**
 * Serves MCP at {@code /mcp} on the DataHub UI host by proxying to the datahub-ai-service.
 *
 * <p>No frontend authentication: datahub-ai-service follows GMS's auth mode and checks the caller's
 * {@code Authorization} header itself, which is forwarded unchanged. The session cookie is never
 * forwarded or turned into a token, so a browser tab logged in to DataHub can't call /mcp with it.
 * Cross-site browser requests also fail their CORS preflight: /mcp sends no CORS headers and
 * answers {@code OPTIONS} with 405.
 *
 * <p>DNS-rebinding protection lives in datahub-ai-service, which knows GMS's auth mode. The
 * frontend passes the evidence through: {@code Origin} unchanged, and the host the client used in
 * {@code X-Forwarded-Host}, overwriting anything the client sent.
 *
 * <p>Responses stream through a non-blocking publisher, so a slow tool call holds no thread from
 * the shared blocking-I/O pool. /mcp has its own admission limit, held until the response finishes
 * streaming, so long MCP calls can't crowd out the UI.
 */
public class AiServiceController extends Controller {

  private static final Logger log = LoggerFactory.getLogger(AiServiceController.class);

  static final String URL_CONFIG = "datahubAiService.url";
  static final String TIMEOUT_CONFIG = "frontend.aiProxy.timeoutSeconds";
  static final String IDLE_TIMEOUT_CONFIG = "frontend.aiProxy.streamIdleTimeoutSeconds";
  static final String MAX_IN_FLIGHT_CONFIG = "frontend.aiProxy.maxInFlight";
  static final int DEFAULT_TIMEOUT_SECONDS = 600;
  static final int DEFAULT_MAX_IN_FLIGHT = 128;

  // Request headers MCP needs, besides every Mcp-* header (session, protocol version, and the
  // Mcp-Method / Mcp-Name / Mcp-Param-* routing headers of the stateless protocol) and the
  // forwarding headers set below. Everything else stays behind: cookies and X-DataHub-Actor
  // above all.
  private static final Set<String> FORWARDED_REQUEST_HEADERS =
      Set.of("authorization", "content-type", "accept", "last-event-id", "user-agent", "origin");
  private static final String MCP_HEADER_PREFIX = "mcp-";

  // Hop-by-hop and framing headers: Play frames the response itself.
  private static final Set<String> DROPPED_RESPONSE_HEADERS =
      Set.of(
          "content-length",
          "content-type",
          "connection",
          "keep-alive",
          "transfer-encoding",
          "content-encoding");

  // A token in the URL ends up in proxy and access logs.
  private static final String TOKEN_QUERY_PARAM_BODY =
      "{\"error\":\"invalid_request\",\"error_description\":\"Send the token in the"
          + " 'Authorization: Bearer <token>' header, not in the URL.\"}";

  private static final String UNAVAILABLE_BODY =
      "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32000,"
          + "\"message\":\"DataHub's AI service is unavailable. Try again shortly.\"}}";

  @Nullable private final URI baseUri;
  private final Duration timeout;
  private final Duration streamIdleTimeout;
  private final HttpClient httpClient;
  private final ProxyAdmission admission;

  @Inject
  public AiServiceController(@Nonnull Config config) {
    this(
        config,
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NEVER)
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  AiServiceController(@Nonnull Config config, @Nonnull HttpClient httpClient) {
    String url = config.hasPath(URL_CONFIG) ? config.getString(URL_CONFIG).trim() : "";
    this.baseUri = url.isEmpty() ? null : URI.create(url.replaceAll("/+$", ""));
    this.timeout = Duration.ofSeconds(positive(config, TIMEOUT_CONFIG, DEFAULT_TIMEOUT_SECONDS));
    this.streamIdleTimeout =
        Duration.ofSeconds(positive(config, IDLE_TIMEOUT_CONFIG, DEFAULT_TIMEOUT_SECONDS));
    // Not bound to readiness: a burst of MCP calls must not take the UI out of rotation.
    this.admission =
        new ProxyAdmission(positive(config, MAX_IN_FLIGHT_CONFIG, DEFAULT_MAX_IN_FLIGHT), null);
    this.httpClient = httpClient;
  }

  private static int positive(Config config, String path, int fallback) {
    if (!config.hasPath(path)) {
      return fallback;
    }
    int value = config.getInt(path);
    return value > 0 ? value : fallback;
  }

  /** {@code OPTIONS /mcp}: no CORS preflight succeeds, so cross-site browser calls fail. */
  public Result rejectPreflight(String path) {
    return Results.status(METHOD_NOT_ALLOWED)
        .withHeader(Http.HeaderNames.ALLOW, "GET, POST, DELETE");
  }

  /**
   * {@code /mcp} and {@code /mcp/<path>} → datahub-ai-service's {@code /mcp} and {@code
   * /mcp/<path>}.
   */
  public CompletionStage<Result> proxyMcp(String path, Http.Request request) {
    if (baseUri == null) {
      return CompletableFuture.completedFuture(notFound());
    }
    if (request.queryString().containsKey("token")) {
      return CompletableFuture.completedFuture(
          badRequest(TOKEN_QUERY_PARAM_BODY).as(Http.MimeTypes.JSON));
    }
    if (!admission.tryAcquire()) {
      return CompletableFuture.completedFuture(admission.overloadedResult());
    }
    Runnable release = releaseOnce();
    try {
      return httpClient
          .sendAsync(buildRequest(path, request), HttpResponse.BodyHandlers.ofPublisher())
          .handle(
              (response, error) -> {
                if (error != null) {
                  release.run();
                  log.warn("datahub-ai-service is unreachable: {}", error.toString());
                  return Results.status(SERVICE_UNAVAILABLE, UNAVAILABLE_BODY)
                      .as(Http.MimeTypes.JSON);
                }
                try {
                  return streamedResult(response, streamIdleTimeout, release);
                } catch (RuntimeException e) {
                  release.run();
                  throw e;
                }
              });
    } catch (RuntimeException e) {
      release.run();
      throw e;
    }
  }

  private Runnable releaseOnce() {
    AtomicBoolean released = new AtomicBoolean();
    return () -> {
      if (released.compareAndSet(false, true)) {
        admission.release();
      }
    };
  }

  HttpRequest buildRequest(String path, Http.Request request) {
    // The query string is dropped: MCP doesn't use one, and ?token= was refused above.
    String suffix = path == null || path.isEmpty() ? "" : "/" + path;
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(URI.create(baseUri + "/mcp" + suffix)).timeout(timeout);

    for (Map.Entry<String, List<String>> header : request.getHeaders().asMap().entrySet()) {
      String name = header.getKey().toLowerCase(Locale.ROOT);
      if (FORWARDED_REQUEST_HEADERS.contains(name) || name.startsWith(MCP_HEADER_PREFIX)) {
        header.getValue().forEach(value -> builder.header(header.getKey(), value));
      }
    }
    // Overwrite, never append: a client-supplied value would let it claim any host.
    request
        .getHeaders()
        .get(Http.HeaderNames.HOST)
        .ifPresent(host -> builder.header("X-Forwarded-Host", host));
    builder.header("X-Forwarded-Proto", request.secure() ? "https" : "http");

    String method = request.method().toUpperCase(Locale.ROOT);
    if ("POST".equals(method)) {
      ByteString body = request.body().asBytes();
      builder.POST(
          HttpRequest.BodyPublishers.ofByteArray(body == null ? new byte[0] : body.toArray()));
    } else {
      builder.method(method, HttpRequest.BodyPublishers.noBody());
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  static Result streamedResult(
      HttpResponse<?> response, Duration streamIdleTimeout, Runnable onFinished) {
    Map<String, String> headers = new HashMap<>();
    response
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              if (!DROPPED_RESPONSE_HEADERS.contains(name.toLowerCase(Locale.ROOT))) {
                headers.put(name, String.join(", ", values));
              }
            });
    // GzipFilter would otherwise compress, and so buffer, the event stream.
    headers.put(Http.HeaderNames.CONTENT_ENCODING, "identity");
    Optional<String> contentType = response.headers().firstValue(Http.HeaderNames.CONTENT_TYPE);

    // The permit is held until the body finishes: completed, failed, idle too long, or cancelled
    // because the client went away, which also cancels the upstream request.
    Source<ByteString, ?> body =
        JavaFlowSupport.Source.fromPublisher((Flow.Publisher<List<ByteBuffer>>) response.body())
            .mapConcat(buffers -> buffers.stream().map(ByteString::fromByteBuffer).toList())
            .idleTimeout(streamIdleTimeout)
            .watchTermination(
                (mat, done) -> {
                  done.whenComplete((ignored, error) -> onFinished.run());
                  return mat;
                });
    return new Result(
        new ResponseHeader(response.statusCode(), headers),
        new HttpEntity.Streamed(body, Optional.empty(), contentType));
  }
}
