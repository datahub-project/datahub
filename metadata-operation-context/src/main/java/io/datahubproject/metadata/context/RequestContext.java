package io.datahubproject.metadata.context;

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

@Getter
@Builder
public class RequestContext implements ContextInterface {
  @Nonnull
  public static final RequestContext TEST =
      RequestContext.builder().requestID("test").requestAPI(RequestAPI.TEST).build();

  @Nonnull private final RequestAPI requestAPI;

  /**
   * i.e. graphql query name or OpenAPI operation id, etc. Intended use case is for log messages and
   * monitoring
   */
  @Nonnull private final String requestID;

  /** Used for ingestion, marks whether the aspect being ingested is going to be validated */
  private final boolean validated;

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }

  public static class RequestContextBuilder {
    private RequestContext build() {
      return new RequestContext(this.requestAPI, this.requestID, this.validated);
    }

    public RequestContext buildGraphql(@Nonnull String queryName, Map<String, Object> variables) {
      requestAPI(RequestAPI.GRAPHQL);
      requestID(buildRequestId(queryName, Set.of()));
      return build();
    }

    public RequestContext buildRestli(
        String action, @Nullable String entityName, boolean validate) {
      return buildRestli(action, entityName == null ? null : List.of(entityName), validate);
    }

    public RequestContext buildRestli(
        @Nonnull String action, @Nullable String[] entityNames, boolean validate) {
      return buildRestli(
          action,
          entityNames == null ? null : Arrays.stream(entityNames).collect(Collectors.toList()),
          validate);
    }

    public RequestContext buildRestli(
        String action, @Nullable Collection<String> entityNames, boolean validate) {
      requestAPI(RequestAPI.RESTLI);
      requestID(buildRequestId(action, entityNames));
      validated(validate);
      return build();
    }

    public RequestContext buildOpenapi(
        @Nonnull String action, @Nullable String entityName, boolean validate) {
      return buildOpenapi(action, entityName == null ? null : List.of(entityName), validate);
    }

    public RequestContext buildOpenapi(
        @Nonnull String action, @Nullable Collection<String> entityNames, boolean validate) {
      requestAPI(RequestAPI.OPENAPI);
      requestID(buildRequestId(action, entityNames));
      validated(validate);
      return build();
    }

    private static String buildRequestId(
        @Nonnull String action, @Nullable Collection<String> entityNames) {
      return entityNames == null || entityNames.isEmpty()
          ? action
          : String.format(
              "%s(%s)", action, entityNames.stream().distinct().collect(Collectors.toList()));
    }
  }

  public enum RequestAPI {
    TEST,
    RESTLI,
    OPENAPI,
    GRAPHQL
  }
}
