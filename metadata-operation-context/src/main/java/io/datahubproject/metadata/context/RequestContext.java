package io.datahubproject.metadata.context;

import com.google.common.net.HttpHeaders;
import com.linkedin.restli.server.ResourceContext;
import io.opentelemetry.api.trace.Span;
import jakarta.servlet.http.HttpServletRequest;
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

@Slf4j
@Getter
@Builder
public class RequestContext implements ContextInterface {
  @Nonnull
  public static final RequestContext TEST =
      RequestContext.builder().requestID("test").requestAPI(RequestAPI.TEST).build();

  @Nonnull private final String actorUrn;
  @Nonnull private final String sourceIP;
  @Nonnull private final RequestAPI requestAPI;

  /**
   * i.e. graphql query name or OpenAPI operation id, etc. Intended use case is for log messages and
   * monitoring
   */
  @Nonnull private final String requestID;

  @Nonnull private final String userAgent;
  @Builder.Default private boolean validated = true;

  public RequestContext(
      @Nonnull String actorUrn,
      @Nonnull String sourceIP,
      @Nonnull RequestAPI requestAPI,
      @Nonnull String requestID,
      @Nonnull String userAgent) {
    this.actorUrn = actorUrn;
    this.sourceIP = sourceIP;
    this.requestAPI = requestAPI;
    this.requestID = requestID;
    this.userAgent = userAgent;
    // Uniform common logging of requests across APIs
    log.info(toString());
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  public static class RequestContextBuilder {
    private RequestContext build() {

      // Add context for tracing
      Span.current()
          .setAttribute("user.id", this.actorUrn)
          .setAttribute("request.api", this.requestAPI.toString())
          .setAttribute("request.id", this.requestID);

      return new RequestContext(
          this.actorUrn, this.sourceIP, this.requestAPI, this.requestID, this.userAgent);
    }

    public RequestContext buildGraphql(
        @Nonnull String actorUrn,
        @Nonnull HttpServletRequest request,
        @Nonnull String queryName,
        Map<String, Object> variables) {
      actorUrn(actorUrn);
      sourceIP(extractSourceIP(request));
      requestAPI(RequestAPI.GRAPHQL);
      requestID(buildRequestId(queryName, Set.of()));
      userAgent(extractUserAgent(request));
      return build();
    }

    public RequestContext buildRestli(
        @Nonnull String actorUrn, @Nullable ResourceContext resourceContext, String action) {
      return buildRestli(actorUrn, resourceContext, action, (String) null);
    }

    public RequestContext buildRestli(
        @Nonnull String actorUrn,
        @Nullable ResourceContext resourceContext,
        String action,
        @Nullable String entityName) {
      return buildRestli(
          actorUrn, resourceContext, action, entityName == null ? null : List.of(entityName));
    }

    public RequestContext buildRestli(
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

    public RequestContext buildRestli(
        @Nonnull String actorUrn,
        @Nullable ResourceContext resourceContext,
        String action,
        @Nullable Collection<String> entityNames) {
      actorUrn(actorUrn);
      sourceIP(resourceContext == null ? "" : extractSourceIP(resourceContext));
      requestAPI(RequestAPI.RESTLI);
      requestID(buildRequestId(action, entityNames));
      userAgent(resourceContext == null ? "" : extractUserAgent(resourceContext));
      return build();
    }

    public RequestContext buildOpenapi(
        @Nonnull String actorUrn,
        @Nonnull HttpServletRequest request,
        @Nonnull String action,
        @Nullable String entityName) {
      return buildOpenapi(
          actorUrn, request, action, entityName == null ? null : List.of(entityName));
    }

    public RequestContext buildOpenapi(
        @Nonnull String actorUrn,
        @Nullable HttpServletRequest request,
        @Nonnull String action,
        @Nullable Collection<String> entityNames) {
      actorUrn(actorUrn);
      sourceIP(request == null ? "" : extractSourceIP(request));
      requestAPI(RequestAPI.OPENAPI);
      requestID(buildRequestId(action, entityNames));
      userAgent(request == null ? "" : extractUserAgent(request));
      return build();
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
        + '}';
  }

  public enum RequestAPI {
    TEST,
    RESTLI,
    OPENAPI,
    GRAPHQL
  }
}
