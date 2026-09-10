package controllers;

import auth.Authenticator;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.typesafe.config.Config;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import play.mvc.Controller;
import play.mvc.Http;
import play.mvc.Result;
import play.mvc.Security;

/**
 * Same-origin proxy for browser-emitted OpenTelemetry spans.
 *
 * <p>The React app's OTLP exporter POSTs to {@code /otel/v1/traces} (same origin as the app) rather
 * than talking to the collector directly. This keeps the collector private (no public ingress), and
 * requires the request to carry a valid DataHub session — reusing the frontend's auth instead of
 * standing up a separate browser-facing auth on the collector.
 *
 * <p>The body (OTLP over HTTP, protobuf or JSON) is forwarded to the internal OTLP/HTTP receiver at
 * {@code otel.exporterOtlpEndpoint} — the observe <b>forwarder</b>, so browser spans enter the same
 * trace-ID load-balanced path as backend spans (correct tail sampling). Forwarding waits for the
 * upstream response so failures can be surfaced: 2xx -> 202; transient failures (429/502/503/504,
 * network, timeout) are retried briefly then reported as retryable so the browser exporter can
 * retry; permanent failures (4xx) are reported as errors so the exporter drops them. Best-effort
 * telemetry — a bounded in-flight limit protects Play threads from a telemetry storm when the
 * collector is slow.
 *
 * <p>Emits metadata-only metrics ({@code otel_proxy_*}); it never logs OTLP bodies, URLs, or query
 * strings.
 */
@Security.Authenticated(Authenticator.class)
public class OtelProxyController extends Controller {

  private static final Logger logger = LoggerFactory.getLogger(OtelProxyController.class.getName());

  private static final String TRACES_SUFFIX = "/v1/traces";

  // OTLP/HTTP retryable statuses (per the OTLP spec). Everything else is treated as permanent.
  private static final Set<Integer> RETRYABLE_STATUSES = Set.of(429, 502, 503, 504);
  private static final Duration[] RETRY_BACKOFFS = {Duration.ofMillis(100), Duration.ofMillis(250)};

  // Metric names (metadata-only).
  private static final String METRIC_SUCCESS = "otel_proxy_forward_success_total";
  private static final String METRIC_FAILURE = "otel_proxy_forward_failure_total";
  private static final String METRIC_REJECTED = "otel_proxy_rejected_total";
  private static final String METRIC_RETRY = "otel_proxy_retry_total";
  private static final String METRIC_INFLIGHT = "otel_proxy_inflight";

  // Collector endpoint, env-driven via BROWSER_OTEL_COLLECTOR_ENDPOINT.
  private static final String ENDPOINT_KEY = "otel.exporterOtlpEndpoint";

  // Matches the frontend Dockerfile's OTEL max payload size (4 MiB). Last line of defense — the
  // parser/ingress should cap oversized bodies before they reach here.
  private static final int MAX_BODY_BYTES = 4 * 1024 * 1024;
  private static final int MAX_IN_FLIGHT = 100;
  private static final int MAX_ATTEMPTS = 3;
  private static final Duration FORWARD_TIMEOUT = Duration.ofSeconds(10);

  private final HttpClient httpClient;
  private final MeterRegistry registry;
  // Full target URI (endpoint + /v1/traces), parsed once at construction so a malformed endpoint
  // fails here — never inside exportTraces after a semaphore permit is acquired (which would leak
  // the permit and eventually wedge the proxy at 503). null when unconfigured/unparseable.
  private final URI targetUri;
  private final Semaphore inFlight;

  @Inject
  public OtelProxyController(
      HttpClient httpClient, @Nonnull Config config, @Nonnull MetricUtils metricUtils) {
    this.httpClient = httpClient;
    this.registry = metricUtils.getRegistry();
    final String endpoint = config.hasPath(ENDPOINT_KEY) ? config.getString(ENDPOINT_KEY) : null;
    this.targetUri = parseTargetUri(endpoint);
    this.inFlight = new Semaphore(MAX_IN_FLIGHT);
    if (registry != null) {
      Gauge.builder(METRIC_INFLIGHT, inFlight, s -> (double) (MAX_IN_FLIGHT - s.availablePermits()))
          .register(registry);
    }
  }

  /**
   * Accepts an OTLP/HTTP trace export from the browser and forwards it to the internal collector.
   *
   * @return 202 once the collector accepts the payload; 400 empty, 413 too large, 415 bad
   *     content-type; 503 (with Retry-After) when unconfigured, overloaded, or forwarding fails
   *     (network); the upstream status verbatim (429/502/503/504, with Retry-After) when the
   *     collector is transiently unavailable; 500 when the collector permanently rejects the
   *     payload.
   */
  @Nonnull
  public CompletableFuture<Result> exportTraces(Http.Request request) {
    if (targetUri == null) {
      logger.warn("Received browser OTLP traces but {} is not configured.", ENDPOINT_KEY);
      count(METRIC_REJECTED, "reason", "no_endpoint");
      return completed(status(SERVICE_UNAVAILABLE, "OTEL collector not configured."));
    }

    final byte[] body =
        request.body().asBytes() != null ? request.body().asBytes().toArray() : new byte[0];
    if (body.length == 0) {
      count(METRIC_REJECTED, "reason", "empty");
      return completed(badRequest("Empty OTLP payload."));
    }
    if (body.length > MAX_BODY_BYTES) {
      count(METRIC_REJECTED, "reason", "too_large");
      return completed(status(REQUEST_ENTITY_TOO_LARGE, "OTLP payload too large."));
    }

    final String contentType = request.contentType().orElse("application/x-protobuf");
    if (!isAllowedContentType(contentType)) {
      count(METRIC_REJECTED, "reason", "bad_type");
      return completed(status(UNSUPPORTED_MEDIA_TYPE, "Unsupported OTLP content type."));
    }

    // Bulkhead: shed load rather than pile up async forwards when the collector is slow/down.
    if (!inFlight.tryAcquire()) {
      count(METRIC_REJECTED, "reason", "overloaded");
      return completed(
          status(SERVICE_UNAVAILABLE, "OTEL proxy overloaded.").withHeader("Retry-After", "1"));
    }

    final HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(targetUri)
            .timeout(FORWARD_TIMEOUT)
            .header(Http.HeaderNames.CONTENT_TYPE, contentType)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body));
    // Preserve the payload encoding so a gzip'd export can be decoded by the collector.
    request
        .header(Http.HeaderNames.CONTENT_ENCODING)
        .ifPresent(enc -> builder.header(Http.HeaderNames.CONTENT_ENCODING, enc));
    final HttpRequest forwardRequest = builder.build();

    return sendWithRetry(forwardRequest, 1)
        .thenApply(this::toResult)
        .exceptionally(
            ex -> {
              Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
              logger.warn(
                  "Failed forwarding browser OTLP traces to collector: {}", cause.getMessage());
              count(METRIC_FAILURE, "reason", "network");
              return status(SERVICE_UNAVAILABLE, "Failed forwarding traces.")
                  .withHeader("Retry-After", "1");
            })
        .whenComplete((result, throwable) -> inFlight.release());
  }

  /** Maps the upstream OTLP response to a status the browser exporter understands. */
  private Result toResult(HttpResponse<Void> response) {
    int code = response.statusCode();
    if (code >= 200 && code < 300) {
      count(METRIC_SUCCESS);
      return status(ACCEPTED);
    }
    if (isRetryableStatus(code)) {
      count(METRIC_FAILURE, "reason", "retryable");
      return status(code, "Collector temporarily unavailable.").withHeader("Retry-After", "1");
    }
    logger.warn("Collector rejected browser OTLP traces with status {}", code);
    count(METRIC_FAILURE, "reason", "permanent");
    return status(INTERNAL_SERVER_ERROR, "Collector rejected traces.");
  }

  /**
   * Forwards the request, retrying with bounded backoff only for transient failures (network,
   * timeout, or a retryable status). Permanent responses are returned as-is for the caller to map.
   */
  private CompletableFuture<HttpResponse<Void>> sendWithRetry(HttpRequest request, int attempt) {
    return httpClient
        .sendAsync(request, HttpResponse.BodyHandlers.discarding())
        .handle(
            (response, throwable) -> {
              boolean canRetry = attempt < MAX_ATTEMPTS;
              if (throwable != null) {
                return canRetry ? delayedRetry(request, attempt) : failedFuture(throwable);
              }
              if (isRetryableStatus(response.statusCode()) && canRetry) {
                return delayedRetry(request, attempt);
              }
              return CompletableFuture.completedFuture(response);
            })
        .thenCompose(future -> future);
  }

  private CompletableFuture<HttpResponse<Void>> delayedRetry(HttpRequest request, int attempt) {
    count(METRIC_RETRY);
    // attempt is 1-based; index into the backoff table, clamping to the last entry.
    Duration delay = RETRY_BACKOFFS[Math.min(attempt - 1, RETRY_BACKOFFS.length - 1)];
    return CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS))
        .thenCompose(ignored -> sendWithRetry(request, attempt + 1));
  }

  private void count(String name, String... tags) {
    if (registry != null) {
      registry.counter(name, tags).increment();
    }
  }

  private static CompletableFuture<HttpResponse<Void>> failedFuture(Throwable throwable) {
    CompletableFuture<HttpResponse<Void>> failed = new CompletableFuture<>();
    failed.completeExceptionally(throwable);
    return failed;
  }

  private static boolean isRetryableStatus(int status) {
    return RETRYABLE_STATUSES.contains(status);
  }

  private static boolean isAllowedContentType(String contentType) {
    String normalized = contentType.toLowerCase();
    return normalized.startsWith("application/x-protobuf")
        || normalized.startsWith("application/json");
  }

  private static CompletableFuture<Result> completed(Result result) {
    return CompletableFuture.completedFuture(result);
  }

  private static String stripTrailingSlash(String url) {
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  /** Parses the collector endpoint into the full traces URI once, tolerating a bad/unset value. */
  private static URI parseTargetUri(String endpoint) {
    if (endpoint == null || endpoint.isBlank()) {
      return null;
    }
    try {
      return URI.create(stripTrailingSlash(endpoint) + TRACES_SUFFIX);
    } catch (IllegalArgumentException e) {
      logger.warn(
          "Invalid {} '{}'; browser trace proxy disabled: {}",
          ENDPOINT_KEY,
          endpoint,
          e.getMessage());
      return null;
    }
  }
}
