package io.datahubproject.metadata.context.request;

import static com.linkedin.metadata.Constants.DATAHUB_ACTOR;
import static com.linkedin.metadata.Constants.DATAHUB_CONTEXT_HEADER_NAME;
import static com.linkedin.metadata.Constants.SERVICE_ACCOUNT_ACTOR_URN_PREFIX;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.REQUEST_API_ATTR;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.REQUEST_ID_ATTR;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.USER_ID_ATTR;

import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.restli.server.ResourceContext;
import io.datahubproject.metadata.context.ContextInterface;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
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
import org.slf4j.MDC;

@Slf4j
@Getter
@Builder(toBuilder = true)
public class RequestContext implements ContextInterface {
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
  @Nonnull private final String agentClass;
  @Nonnull private final String agentName;

  /**
   * Parsed {@value com.linkedin.metadata.Constants#DATAHUB_CONTEXT_HEADER_NAME} dimensions for
   * metrics and tracing (policy from {@link DataHubContextRulesHolder} at parse time).
   */
  @Nonnull private final DataHubContextParser.Parsed dataHubContextParsed;

  @Nullable private final MetricUtils metricUtils;
  @Nullable private final String traceId;

  public RequestContext(
      @Nullable MetricUtils metricUtils,
      @Nonnull String actorUrn,
      @Nonnull String sourceIP,
      @Nonnull RequestAPI requestAPI,
      @Nonnull String requestID,
      @Nonnull String userAgent,
      @Nonnull DataHubContextParser.Parsed dataHubContextParsed) {
    this.actorUrn = actorUrn;
    this.sourceIP = sourceIP;
    this.requestAPI = requestAPI;
    this.requestID = requestID;
    this.userAgent = userAgent;
    this.dataHubContextParsed = dataHubContextParsed;
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
      this.agentClass = ua.get(UserAgent.AGENT_CLASS).getValue();
      this.agentName = ua.get(UserAgent.AGENT_NAME).getValue();
    } else {
      this.agentClass = "Unknown";
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

  @Nonnull
  public String getContextSkill() {
    return dataHubContextParsed.getSkill();
  }

  @Nonnull
  public String getContextCaller() {
    return dataHubContextParsed.getCaller();
  }

  public static class RequestContextBuilder {

    @Nullable private String dataHubContextHeader;

    public RequestContextBuilder dataHubContextHeader(@Nullable String dataHubContextHeader) {
      this.dataHubContextHeader = dataHubContextHeader;
      return this;
    }

    public RequestContext build() {
      DataHubContextParser.Parsed dhContext =
          DataHubContextParser.parse(this.dataHubContextHeader, DataHubContextRulesHolder.get());
      Span currentSpan = Span.current();
      if (currentSpan != null) {
        currentSpan
            .setAttribute(USER_ID_ATTR, this.actorUrn)
            .setAttribute(REQUEST_API_ATTR, this.requestAPI.toString())
            .setAttribute(REQUEST_ID_ATTR, this.requestID);
        String unspecified = dhContext.getPolicy().getUnspecifiedLabel();
        for (DataHubContextKeyRule rule : dhContext.getPolicy().getRules()) {
          String v = dhContext.getForHeaderKey(rule.getHeaderKey());
          if (!unspecified.equals(v)) {
            currentSpan.setAttribute("context." + rule.getHeaderKey(), v);
          }
        }
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
          dhContext);
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
      dataHubContextHeader(extractDataHubContext(request));
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
      dataHubContextHeader(resourceContext == null ? null : extractDataHubContext(resourceContext));
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
      dataHubContextHeader(request == null ? null : extractDataHubContext(request));
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

    private static String extractDataHubContext(@Nonnull HttpServletRequest request) {
      return Optional.ofNullable(request.getHeader(DATAHUB_CONTEXT_HEADER_NAME)).orElse(null);
    }

    private static String extractDataHubContext(@Nonnull ResourceContext resourceContext) {
      return Optional.ofNullable(
              resourceContext.getRequestHeaders().get(DATAHUB_CONTEXT_HEADER_NAME))
          .orElse(null);
    }
  }

  private static void captureAPIMetrics(MetricUtils metricUtils, RequestContext requestContext) {
    final String userCategory;
    if (SYSTEM_ACTOR.equals(requestContext.actorUrn)) {
      userCategory = "system";
    } else if (DATAHUB_ACTOR.equals(requestContext.actorUrn)) {
      userCategory = "admin";
    } else if (requestContext.actorUrn.startsWith(SERVICE_ACCOUNT_ACTOR_URN_PREFIX)) {
      // Matches corpusers provisioned via ServiceAccountService (urn:li:corpuser:service:...);
      // no SubTypes fetch on the request path.
      userCategory = "service";
    } else {
      userCategory = "regular";
    }

    if (requestContext.getRequestAPI() != RequestAPI.TEST && metricUtils != null) {
      String agentClass = requestContext.getAgentClass().toLowerCase().replaceAll("\\s+", "");
      String requestAPIStr = requestContext.getRequestAPI().toString().toLowerCase();
      metricUtils.increment(
          String.format("requestContext_%s_%s_%s", userCategory, agentClass, requestAPIStr), 1);
      List<String> tagList = new ArrayList<>(8);
      tagList.add(MetricUtils.TAG_REQUEST_USER_CATEGORY);
      tagList.add(userCategory);
      tagList.add(MetricUtils.TAG_REQUEST_AGENT_CLASS);
      tagList.add(agentClass);
      tagList.add(MetricUtils.TAG_REQUEST_API);
      tagList.add(requestAPIStr);
      String[] ctxPairs = requestContext.getDataHubContextParsed().flatMicrometerTagPairs();
      for (String p : ctxPairs) {
        tagList.add(p);
      }
      metricUtils.incrementMicrometer(
          MetricUtils.DATAHUB_API_TRAFFIC, 1, tagList.toArray(new String[0]));
    }
  }

  @Override
  public String toString() {
    return "RequestContext{"
        + "actorUrn='"
        + actorUrn
        + '\''
        + ", sourceIP='"
        + sourceIP
        + '\''
        + ", requestAPI="
        + requestAPI
        + ", requestID='"
        + requestID
        + '\''
        + ", userAgent='"
        + userAgent
        + '\''
        + ", agentClass='"
        + agentClass
        + '\''
        + ", contextSkill='"
        + getContextSkill()
        + '\''
        + ", contextCaller='"
        + getContextCaller()
        + '\''
        + ", traceId='"
        + traceId
        + '\''
        + '}';
  }

  public enum RequestAPI {
    TEST,
    RESTLI,
    OPENAPI,
    GRAPHQL
  }
}
