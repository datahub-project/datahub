package io.datahubproject.metadata.context;

import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.REQUEST_API_ATTR;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.REQUEST_ID_ATTR;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.USER_ID_ATTR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.restli.server.ResourceContext;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.usage.AuthChannel;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nl.basjes.parse.useragent.UserAgent;
import nl.basjes.parse.useragent.UserAgentAnalyzer;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.MDC;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class RequestContext implements ContextInterface {
  public static final String MDC_USAGE_OPERATION = "usageOperation";
  public static final String MDC_AUTH_CHANNEL = "authChannel";
  public static final String MDC_USAGE_QUANTITY = "usageQuantity";

  public static final UserAgentAnalyzer UAA =
      UserAgentAnalyzer.newBuilder()
          .hideMatcherLoadStats()
          .addResources("datahub_user_agents.yaml")
          .withFields(UserAgent.AGENT_CLASS, UserAgent.AGENT_NAME)
          .withCache(1000)
          .build();

  @Nonnull
  public static final RequestContextBuilder TEST =
      RequestContext.builder()
          .actorUrn("")
          .sourceIP("")
          .userAgent("")
          .requestID("test")
          .requestAPI(RequestAPI.TEST);

  @Nonnull private final String actorUrn;
  @Nonnull private final String sourceIP;
  @Nonnull private final RequestAPI requestAPI;

  /**
   * i.e. graphql query name or OpenAPI operation id, etc. Intended use case is for log messages and
   * monitoring
   */
  @Nonnull private final String requestID;

  @Nonnull private final String userAgent;
  @Nonnull private final AgentClass agentClass;
  @Nonnull private final String agentName;
  @Nullable private final MetricUtils metricUtils;
  @Nullable private final String traceId;

  /** Surface-neutral usage operation registry key (e.g. dimensions.usage_operation). */
  @Nullable private final String usageOperation;

  /** Stable usage identity string for distinct actor counting (MAU-style metrics). */
  @Nullable private final String usageIdentity;

  @Nullable private final AuthChannel authChannel;
  @Nullable private final GraphQLOperationKind graphqlOperationKind;

  /** Decoded request body length when fully materialized; null when omitted (streaming). */
  @Nullable private final Long inputBytes;

  /** Decoded response body length when fully materialized; null when omitted. */
  @Nullable private final Long outputBytes;

  /** Whether the request body was fully buffered for byte measurement. */
  private final boolean requestBodyMaterialized;

  /** Whether the response body was fully buffered for byte measurement. */
  private final boolean responseBodyMaterialized;

  /** Multiplier for per-proposal ingest cost profiling (batch size). Defaults to 1. */
  @Builder.Default private final long usageQuantity = 1L;

  public RequestContext(
      MetricUtils metricUtils,
      @Nonnull String actorUrn,
      @Nonnull String sourceIP,
      @Nonnull RequestAPI requestAPI,
      @Nonnull String requestID,
      @Nonnull String userAgent) {
    this(
        metricUtils,
        actorUrn,
        sourceIP,
        requestAPI,
        requestID,
        userAgent,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        false,
        1);
  }

  public RequestContext(
      MetricUtils metricUtils,
      @Nonnull String actorUrn,
      @Nonnull String sourceIP,
      @Nonnull RequestAPI requestAPI,
      @Nonnull String requestID,
      @Nonnull String userAgent,
      @Nullable String usageOperation,
      @Nullable String usageIdentity,
      @Nullable AuthChannel authChannel,
      @Nullable GraphQLOperationKind graphqlOperationKind,
      @Nullable Long inputBytes,
      @Nullable Long outputBytes,
      boolean requestBodyMaterialized,
      boolean responseBodyMaterialized) {
    this(
        metricUtils,
        actorUrn,
        sourceIP,
        requestAPI,
        requestID,
        userAgent,
        usageOperation,
        usageIdentity,
        authChannel,
        graphqlOperationKind,
        inputBytes,
        outputBytes,
        requestBodyMaterialized,
        responseBodyMaterialized,
        1);
  }

  public RequestContext(
      MetricUtils metricUtils,
      @Nonnull String actorUrn,
      @Nonnull String sourceIP,
      @Nonnull RequestAPI requestAPI,
      @Nonnull String requestID,
      @Nonnull String userAgent,
      @Nullable String usageOperation,
      @Nullable String usageIdentity,
      @Nullable AuthChannel authChannel,
      @Nullable GraphQLOperationKind graphqlOperationKind,
      @Nullable Long inputBytes,
      @Nullable Long outputBytes,
      boolean requestBodyMaterialized,
      boolean responseBodyMaterialized,
      long usageQuantity) {
    this.actorUrn = actorUrn;
    this.sourceIP = sourceIP;
    this.requestAPI = requestAPI;
    this.requestID = requestID;
    this.userAgent = userAgent;
    this.metricUtils = metricUtils;

    /*
     *         "Browser",
     *         "Robot",
     *         "Crawler",
     *         "Mobile App",
     *         "Email Client",
     *         "Library",
     *         "Hacker",
     *         "Unknown",
     *         // DataHub Specific below
     *         "CLI",
     *         "INGESTION",
     *         "SDK"
     */
    if (this.userAgent != null && !this.userAgent.isEmpty()) {
      UserAgent ua = UAA.parse(this.userAgent);
      this.agentClass = AgentClass.fromRawUserAgentClass(ua.get(UserAgent.AGENT_CLASS).getValue());
      this.agentName = ua.get(UserAgent.AGENT_NAME).getValue();
    } else {
      this.agentClass = AgentClass.UNKNOWN;
      this.agentName = "Unknown";
    }

    Span currentSpan = Span.current();
    String traceId = null;
    if (currentSpan != null) {
      SpanContext spanContext = currentSpan.getSpanContext();
      if (spanContext != null && spanContext.isValid()) {
        traceId = spanContext.getTraceId();
        MDC.put("traceId", traceId);
      }
    }
    this.traceId = traceId;
    this.usageOperation = usageOperation;
    this.usageIdentity = usageIdentity;
    this.authChannel = authChannel;
    this.graphqlOperationKind = graphqlOperationKind;
    this.inputBytes = inputBytes;
    this.outputBytes = outputBytes;
    this.requestBodyMaterialized = requestBodyMaterialized;
    this.responseBodyMaterialized = responseBodyMaterialized;
    this.usageQuantity = Math.max(1L, usageQuantity);

    putUsageFieldsInMdc();

    // Uniform common logging of requests across APIs
    log.debug("{}", this);

    // API metrics
    if (metricUtils != null) {
      captureAPIMetrics(metricUtils, this);
    }
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  /** Counts JSON array elements for ingest batch usage quantity; returns 1 when not an array. */
  public static long resolveIngestUsageQuantity(
      @Nonnull String jsonBody, @Nonnull ObjectMapper objectMapper) {
    try {
      JsonNode node = objectMapper.readTree(jsonBody);
      if (node.isArray()) {
        return Math.max(1L, node.size());
      }
    } catch (JsonProcessingException e) {
      log.debug("Could not parse ingest body for usage quantity", e);
    }
    return 1L;
  }

  /** Sums array sizes under a JSON object keyed by entity type (generic multi-type ingest). */
  public static long resolveIngestUsageQuantity(@Nonnull JsonNode rootObject) {
    if (!rootObject.isObject()) {
      return 1L;
    }
    long total = 0L;
    for (JsonNode value : rootObject) {
      if (value.isArray()) {
        total += value.size();
      } else {
        total += 1L;
      }
    }
    return Math.max(1L, total);
  }

  /** Resolves request wire bytes from servlet {@code Content-Length} when not streaming/chunked. */
  @Nullable
  public static Long resolveRequestInputBytes(@Nonnull HttpServletRequest request) {
    return wireBytesFromContentLength(
        request.getContentLengthLong(), isStreamingOrChunkedHttpRequest(request));
  }

  /** Resolves request wire bytes from Rest.li {@code Content-Length} request headers only. */
  @Nullable
  public static Long resolveRequestInputBytes(@Nullable ResourceContext resourceContext) {
    if (resourceContext == null) {
      return null;
    }
    Map<String, String> headers = resourceContext.getRequestHeaders();
    long contentLength = parseContentLengthHeader(headers.get(HttpHeaders.CONTENT_LENGTH));
    return wireBytesFromContentLength(
        contentLength, isChunkedTransferEncoding(headers.get(HttpHeaders.TRANSFER_ENCODING)));
  }

  /** Resolves response wire bytes from {@code Content-Length} when not streaming/chunked. */
  @Nullable
  public static Long resolveResponseOutputBytes(@Nonnull HttpServletResponse response) {
    return resolveResponseOutputBytes(response, 0L);
  }

  /**
   * Resolves response bytes, preferring measured bytes written to the response body when {@code
   * measuredBytesWritten} is positive (for example from {@link
   * CountingHttpServletResponseWrapper}). Falls back to {@code Content-Length} when no body bytes
   * were measured.
   */
  @Nullable
  public static Long resolveResponseOutputBytes(
      @Nonnull HttpServletResponse response, long measuredBytesWritten) {
    if (measuredBytesWritten > 0) {
      return measuredBytesWritten;
    }
    long contentLength = parseContentLengthHeader(response.getHeader(HttpHeaders.CONTENT_LENGTH));
    boolean streaming =
        contentLength < 0
            || isChunkedTransferEncoding(response.getHeader(HttpHeaders.TRANSFER_ENCODING));
    return wireBytesFromContentLength(contentLength, streaming);
  }

  @Nullable
  static Long wireBytesFromContentLength(long contentLength, boolean streamingOrChunked) {
    if (streamingOrChunked || contentLength < 0) {
      return null;
    }
    return contentLength;
  }

  static long parseContentLengthHeader(@Nullable String header) {
    if (header == null || header.isBlank()) {
      return -1L;
    }
    return NumberUtils.toLong(header.trim(), -1L);
  }

  static boolean isChunkedTransferEncoding(@Nullable String transferEncoding) {
    return transferEncoding != null && transferEncoding.toLowerCase().contains("chunked");
  }

  static boolean isStreamingOrChunkedHttpRequest(@Nonnull HttpServletRequest request) {
    if (request.getContentLengthLong() < 0) {
      return true;
    }
    return isChunkedTransferEncoding(request.getHeader(HttpHeaders.TRANSFER_ENCODING));
  }

  public static class RequestContextBuilder {

    private long usageQuantity = 1L;

    public RequestContextBuilder usageQuantity(long usageQuantity) {
      this.usageQuantity = Math.max(1L, usageQuantity);
      return this;
    }

    public RequestContext build() {
      // Add context for tracing
      Span currentSpan = Span.current();
      if (currentSpan != null) {
        currentSpan
            .setAttribute(USER_ID_ATTR, this.actorUrn)
            .setAttribute(REQUEST_API_ATTR, this.requestAPI.toString())
            .setAttribute(REQUEST_ID_ATTR, this.requestID);
      }
      Optional.ofNullable(Context.current().get(SystemTelemetryContext.EVENT_SOURCE_CONTEXT_KEY))
          .ifPresent(eventSource -> eventSource.set(requestAPI.toString()));
      Optional.ofNullable(Context.current().get(SystemTelemetryContext.SOURCE_IP_CONTEXT_KEY))
          .ifPresent(eventSource -> eventSource.set(sourceIP));

      return new RequestContext(
          this.metricUtils,
          this.actorUrn,
          this.sourceIP,
          this.requestAPI,
          this.requestID,
          this.userAgent,
          this.usageOperation,
          this.usageIdentity,
          this.authChannel,
          this.graphqlOperationKind,
          this.inputBytes,
          this.outputBytes,
          this.requestBodyMaterialized,
          this.responseBodyMaterialized,
          this.usageQuantity);
    }

    public RequestContextBuilder buildGraphql(
        @Nonnull String actorUrn,
        @Nonnull HttpServletRequest request,
        @Nonnull String queryName,
        Map<String, Object> variables) {
      actorUrn(actorUrn);
      sourceIP(extractSourceIP(request));
      requestAPI(RequestAPI.GRAPHQL);
      requestID(buildRequestId(queryName, Set.of()));
      userAgent(extractUserAgent(request));
      if (peekInputBytes() == null) {
        withWireInput(request);
      }
      return this;
    }

    public RequestContextBuilder buildRestli(
        @Nonnull String actorUrn, @Nullable ResourceContext resourceContext, String action) {
      return buildRestli(actorUrn, resourceContext, action, (String) null);
    }

    public RequestContextBuilder buildRestli(
        @Nonnull String actorUrn,
        @Nullable ResourceContext resourceContext,
        String action,
        @Nullable String entityName) {
      return buildRestli(
          actorUrn, resourceContext, action, entityName == null ? null : List.of(entityName));
    }

    public RequestContextBuilder buildRestli(
        @Nonnull String actorUrn,
        @Nullable ResourceContext resourceContext,
        @Nonnull String action,
        @Nullable String[] entityNames) {
      return buildRestli(
          actorUrn,
          resourceContext,
          action,
          entityNames == null ? null : Arrays.stream(entityNames).collect(Collectors.toList()));
    }

    public RequestContextBuilder buildRestli(
        @Nonnull String actorUrn,
        @Nullable ResourceContext resourceContext,
        String action,
        @Nullable Collection<String> entityNames) {
      actorUrn(actorUrn);
      sourceIP(resourceContext == null ? "" : extractSourceIP(resourceContext));
      requestAPI(RequestAPI.RESTLI);
      requestID(buildRequestId(action, entityNames));
      userAgent(resourceContext == null ? "" : extractUserAgent(resourceContext));
      if (resourceContext != null && peekInputBytes() == null) {
        withWireInput(resourceContext);
      }
      return this;
    }

    public RequestContextBuilder buildOpenapi(
        @Nonnull String actorUrn,
        @Nonnull HttpServletRequest request,
        @Nonnull String action,
        @Nullable String entityName) {
      return buildOpenapi(
          actorUrn, request, action, entityName == null ? null : List.of(entityName));
    }

    public RequestContextBuilder buildOpenapi(
        @Nonnull String actorUrn,
        @Nullable HttpServletRequest request,
        @Nonnull String action,
        @Nullable Collection<String> entityNames) {
      actorUrn(actorUrn);
      sourceIP(request == null ? "" : extractSourceIP(request));
      requestAPI(RequestAPI.OPENAPI);
      requestID(buildRequestId(action, entityNames));
      userAgent(request == null ? "" : extractUserAgent(request));
      if (request != null && peekInputBytes() == null) {
        withWireInput(request);
      }
      return this;
    }

    private static String buildRequestId(
        @Nonnull String action, @Nullable Collection<String> entityNames) {
      return entityNames == null || entityNames.isEmpty()
          ? action
          : String.format(
              "%s(%s)", action, entityNames.stream().distinct().collect(Collectors.toList()));
    }

    private static String extractUserAgent(@Nonnull HttpServletRequest request) {
      return Optional.ofNullable(request.getHeader(HttpHeaders.USER_AGENT)).orElse("");
    }

    private static String extractUserAgent(@Nonnull ResourceContext resourceContext) {
      return Optional.ofNullable(resourceContext.getRequestHeaders().get(HttpHeaders.USER_AGENT))
          .orElse("");
    }

    private static String extractSourceIP(@Nonnull HttpServletRequest request) {
      return Optional.ofNullable(request.getHeader(HttpHeaders.X_FORWARDED_FOR))
          .orElse(request.getRemoteAddr());
    }

    private static String extractSourceIP(@Nonnull ResourceContext resourceContext) {
      return Optional.ofNullable(
              resourceContext.getRequestHeaders().get(HttpHeaders.X_FORWARDED_FOR))
          .orElse(resourceContext.getRawRequestContext().getLocalAttr("REMOTE_ADDR").toString());
    }

    public RequestAPI peekRequestAPI() {
      return this.requestAPI;
    }

    public String peekRequestID() {
      return this.requestID;
    }

    public String peekActorUrn() {
      return this.actorUrn;
    }

    public String peekUserAgent() {
      return this.userAgent;
    }

    public GraphQLOperationKind peekGraphqlOperationKind() {
      return this.graphqlOperationKind;
    }

    public Long peekInputBytes() {
      return this.inputBytes;
    }

    @Nullable
    public String peekUsageOperation() {
      return this.usageOperation;
    }

    @Nonnull
    public RequestContextBuilder withClientHeaders(
        @Nullable String xForwardedFor, @Nullable String userAgentHeader) {
      if (xForwardedFor != null && !xForwardedFor.isEmpty()) {
        sourceIP(xForwardedFor);
      }
      if (userAgentHeader != null && !userAgentHeader.isEmpty()) {
        userAgent(userAgentHeader);
      }
      return this;
    }

    @Nonnull
    public RequestContextBuilder withUsageOperation(@Nonnull UsageOperation operation) {
      return usageOperation(operation.key());
    }

    @Nonnull
    public RequestContextBuilder withUsageQuantity(long quantity) {
      return usageQuantity(Math.max(1L, quantity));
    }

    @Nonnull
    public RequestContextBuilder withMaterializedOutputBytes(long outputByteCount) {
      if (outputByteCount <= 0) {
        return this;
      }
      outputBytes(outputByteCount);
      responseBodyMaterialized(true);
      return this;
    }

    @Nonnull
    public RequestContextBuilder withWireInput(@Nonnull HttpServletRequest request) {
      if (peekInputBytes() != null) {
        return this;
      }
      Long inputByteCount = resolveRequestInputBytes(request);
      if (inputByteCount != null) {
        inputBytes(inputByteCount);
        requestBodyMaterialized(true);
      }
      return this;
    }

    @Nonnull
    public RequestContextBuilder withWireInput(@Nullable ResourceContext resourceContext) {
      if (peekInputBytes() != null) {
        return this;
      }
      Long inputByteCount = resolveRequestInputBytes(resourceContext);
      if (inputByteCount != null) {
        inputBytes(inputByteCount);
        requestBodyMaterialized(true);
      }
      return this;
    }

    /**
     * Records input size from an already-parsed UTF-8 request body (for example
     * {@code @RequestBody} {@code String} on OpenAPI ingest handlers). Fills the gap when {@link
     * #withWireInput} cannot use {@code Content-Length} because the client sent chunked transfer
     * encoding.
     */
    @Nonnull
    public RequestContextBuilder withMaterializedInputUtf8Bytes(@Nonnull String bodyUtf8) {
      if (peekInputBytes() != null || bodyUtf8.isEmpty()) {
        return this;
      }
      inputBytes((long) bodyUtf8.getBytes(StandardCharsets.UTF_8).length);
      requestBodyMaterialized(true);
      return this;
    }
  }

  private static void captureAPIMetrics(MetricUtils metricUtils, RequestContext requestContext) {
    final String userCategory =
        UsageActorClass.fromActorUrn(requestContext.actorUrn).toLegacyUserCategoryTag();

    if (requestContext.getRequestAPI() != RequestAPI.TEST && metricUtils != null) {
      String agentClass = requestContext.getAgentClass().toMetricLabel();
      String requestAPI = requestContext.getRequestAPI().toMetricLabel();
      metricUtils.increment(
          String.format("requestContext_%s_%s_%s", userCategory, agentClass, requestAPI), 1);
      // Per-request Micrometer counter. Skip when aggregation Micrometer export owns
      // datahub_request_count (flush uses billing tag keys; Micrometer forbids mixed tag sets).
      if (!metricUtils.isSuppressLegacyRequestCountMicrometer()) {
        metricUtils.incrementMicrometer(
            MetricUtils.DATAHUB_REQUEST_COUNT,
            1,
            "user_category",
            userCategory,
            "agent_class",
            agentClass,
            "request_api",
            requestAPI);
      }
    }
  }

  /** Populates MDC with low-cardinality usage dimensions when this request is tagged. */
  private void putUsageFieldsInMdc() {
    if (usageOperation == null || usageOperation.isEmpty()) {
      return;
    }
    MDC.put(MDC_USAGE_OPERATION, usageOperation);
    if (authChannel != null) {
      MDC.put(MDC_AUTH_CHANNEL, authChannel.dimensionValue());
    }
    if (usageQuantity > 1) {
      MDC.put(MDC_USAGE_QUANTITY, String.valueOf(usageQuantity));
    }
  }

  /** Clears usage MDC keys; call at end of servlet request to avoid thread-pool leakage. */
  public static void clearUsageFieldsFromMdc() {
    MDC.remove(MDC_USAGE_OPERATION);
    MDC.remove(MDC_AUTH_CHANNEL);
    MDC.remove(MDC_USAGE_QUANTITY);
  }

  @Override
  public String toString() {
    StringBuilder sb =
        new StringBuilder("RequestContext{")
            .append("actorUrn='")
            .append(actorUrn)
            .append('\'')
            .append(", sourceIP='")
            .append(sourceIP)
            .append('\'')
            .append(", requestAPI=")
            .append(requestAPI)
            .append(", requestID='")
            .append(requestID)
            .append('\'')
            .append(", userAgent='")
            .append(userAgent)
            .append('\'')
            .append(", agentClass=")
            .append(agentClass)
            .append(", traceId='")
            .append(traceId)
            .append('\'');
    appendUsageFieldsToString(sb);
    return sb.append('}').toString();
  }

  private void appendUsageFieldsToString(StringBuilder sb) {
    if (usageOperation == null || usageOperation.isEmpty()) {
      return;
    }
    sb.append(", usageOperation='").append(usageOperation).append('\'');
    if (authChannel != null) {
      sb.append(", authChannel=").append(authChannel);
    }
    if (graphqlOperationKind != null) {
      sb.append(", graphqlOperationKind=").append(graphqlOperationKind);
    }
    if (inputBytes != null) {
      sb.append(", inputBytes=").append(inputBytes);
    }
    if (usageQuantity > 1) {
      sb.append(", usageQuantity=").append(usageQuantity);
    }
  }

  public enum RequestAPI {
    TEST,
    RESTLI,
    OPENAPI,
    GRAPHQL,
    /**
     * MetadataChangeProposal queue consumption (Kafka / pgQueue on the MCE consumer). Distinct from
     * {@link #MCP} (Model Context Protocol server traffic).
     */
    MESSAGING,
    /**
     * Model Context Protocol server traffic (report-driven metering). Distinct from {@link
     * #MESSAGING} (MetadataChangeProposal ingest).
     */
    MCP;

    @Nonnull
    public String toMetricLabel() {
      return name().toLowerCase();
    }
  }
}
