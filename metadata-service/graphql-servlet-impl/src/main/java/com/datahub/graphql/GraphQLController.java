package com.datahub.graphql;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.telemetry.OpenTelemetryKeyConstants.ACTOR_URN_ATTR;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.inject.name.Named;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLError;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.ratelimit.ClientClass;
import com.linkedin.metadata.ratelimit.ClientClassifier;
import com.linkedin.metadata.ratelimit.GraphqlDocumentAnalyzer;
import com.linkedin.metadata.ratelimit.GraphqlDocumentMetadata;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitHeaderWriter;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitLease;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.opentelemetry.api.trace.Span;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api")
public class GraphQLController {

  @Inject GraphQLEngine _engine;

  @Inject AuthorizerChain _authorizerChain;

  @Inject ConfigurationProvider configurationProvider;

  @Inject MetricUtils metricUtils;

  @Inject RateLimitEngine rateLimitEngine;

  @Inject GraphqlUsageClassificationRegistry graphqlUsageClassificationRegistry;

  @Autowired(required = false)
  UsageMetricsSessionEnricher usageMetricsSessionEnricher;

  @Nonnull
  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  private static final int MAX_LOG_WIDTH = 512;

  /**
   * Serializes GraphQL execution results. Must not use {@link JsonInclude.Include#NON_NULL} —
   * nullable fields are returned as explicit JSON nulls, and clients rely on that shape.
   */
  private static final ObjectMapper GRAPHQL_RESPONSE_MAPPER = createGraphQLResponseMapper();

  private static ObjectMapper createGraphQLResponseMapper() {
    ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    int maxNameLength =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_NAME_LENGTH, MAX_JACKSON_NAME_LENGTH));
    mapper
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder()
                .maxStringLength(maxSize)
                .maxNameLength(maxNameLength)
                .build());
    mapper.registerModule(new Jdk8Module());
    return mapper;
  }

  /**
   * Part B heavy-resolver gate. When the front gate admitted the request, consumes each configured
   * heavy top-level resolver's bucket (in query order). On the first denial — or if a resolver
   * evaluation throws with fail-open disabled — it unwinds everything the request already acquired:
   * the held front-gate capacity slot, the scoped-chain tokens ({@code actorUrn}/{@code
   * clientClass}), and any heavy-resolver buckets already charged earlier in this same request. So
   * a request rejected (or erroring) here neither leaks a capacity slot nor permanently burns the
   * actor's scoped or per-resolver quota. Otherwise returns the original decision unchanged.
   * Package-private + static so the wiring is unit-testable without the full request pipeline.
   */
  static RateLimitDecision applyHeavyResolverGate(
      @Nonnull RateLimitEngine rateLimitEngine,
      @Nonnull RateLimitDecision frontGateDecision,
      @Nonnull List<String> topLevelFields,
      boolean systemActor,
      @Nullable String actorUrn,
      @Nullable ClientClass clientClass) {
    if (!frontGateDecision.isAllowed()) {
      return frontGateDecision;
    }
    // Resolvers we've passed so far this request; their buckets are refunded if a later one denies.
    // refundHeavyResolver is guarded + capacity-capped, so listing non-charged fields is harmless.
    List<String> chargedResolvers = new ArrayList<>(topLevelFields.size());
    for (String topLevelField : topLevelFields) {
      RateLimitDecision heavyDecision;
      try {
        heavyDecision = rateLimitEngine.consumeHeavyResolver(topLevelField, systemActor);
      } catch (RuntimeException e) {
        // Fail-open disabled and evaluation threw: release the front gate we're still holding
        // (otherwise the capacity slot leaks) and refund what we consumed, then propagate.
        unwindHeavyGate(
            rateLimitEngine,
            frontGateDecision,
            actorUrn,
            clientClass,
            chargedResolvers,
            systemActor);
        throw e;
      }
      if (heavyDecision != null && !heavyDecision.isAllowed()) {
        unwindHeavyGate(
            rateLimitEngine,
            frontGateDecision,
            actorUrn,
            clientClass,
            chargedResolvers,
            systemActor);
        return heavyDecision;
      }
      chargedResolvers.add(topLevelField);
    }
    return frontGateDecision;
  }

  /**
   * Rolls back the front-gate capacity, the scoped chain, and any charged heavy-resolver buckets.
   */
  private static void unwindHeavyGate(
      RateLimitEngine rateLimitEngine,
      RateLimitDecision frontGateDecision,
      @Nullable String actorUrn,
      @Nullable ClientClass clientClass,
      List<String> chargedResolvers,
      boolean systemActor) {
    rateLimitEngine.releaseCapacity(frontGateDecision, false);
    rateLimitEngine.refundScopedChain(actorUrn, clientClass);
    for (String resolver : chargedResolvers) {
      rateLimitEngine.refundHeavyResolver(resolver, systemActor);
    }
  }

  /** 429 response for a rate-limit denial, carrying the decision's throttle headers. */
  private static CompletableFuture<ResponseEntity<String>> tooManyRequests(
      @Nonnull RateLimitDecision decision, @Nonnull ObjectMapper mapper) {
    HttpHeaders headers = new HttpHeaders();
    RateLimitHeaderWriter.createHeaders(decision).forEach(headers::add);
    try {
      return CompletableFuture.completedFuture(
          new ResponseEntity<>(
              mapper.writeValueAsString(Map.of("error", "Rate limit exceeded")),
              headers,
              HttpStatus.TOO_MANY_REQUESTS));
    } catch (JsonProcessingException e) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.TOO_MANY_REQUESTS));
    }
  }

  @PostMapping(value = "/graphql", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> postGraphQL(
      HttpServletRequest request, HttpEntity<String> httpEntity) {

    String jsonStr = httpEntity.getBody();
    ObjectMapper mapper = systemOperationContext.getObjectMapper();
    JsonNode bodyJson = null;
    try {
      bodyJson = mapper.readTree(jsonStr);
    } catch (JsonProcessingException e) {
      log.error("Failed to parse json ", e);
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    if (bodyJson == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    /*
     * Extract "query" field
     */
    JsonNode queryJson = bodyJson.get("query");
    if (queryJson == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }
    final String query = queryJson.asText();

    /*
     * Extract "operationName" field
     */
    JsonNode operationNameJson = bodyJson.get("operationName");
    final String operationName =
        (operationNameJson != null && !operationNameJson.isNull())
            ? operationNameJson.asText()
            : null;
    // Single parse of the query for the entire rate-limit path: the query/display name, the
    // rate-limit identity, and the top-level resolver names all come from this one analyze() call —
    // no re-parsing the document per consumer.
    final GraphqlDocumentMetadata documentMetadata =
        GraphqlDocumentAnalyzer.analyze(
            operationName,
            query,
            name -> graphqlUsageClassificationRegistry.resolveByOperationName(name).isPresent());

    /*
     * Extract "variables" map
     */
    JsonNode variablesJson = bodyJson.get("variables");
    final Map<String, Object> variables =
        (variablesJson != null && !variablesJson.isNull())
            ? mapper.convertValue(variablesJson, new TypeReference<Map<String, Object>>() {})
            : Collections.emptyMap();

    Authentication authentication = AuthenticationContext.getAuthentication();

    // Per-actor rate limiting keys on the authenticated actor urn, which is available for every
    // actor type via the authentication — unlike context.getActorUrn(), which throws for non-USER
    // actors and would (via a catch-all) silently skip the per-actor bucket, handing service/role
    // principals a free pass. Only the internal system principal is exempt, so its high-volume
    // internal calls aren't per-actor throttled (mirrors DataHubAuthorizer.isSystemRequest);
    // everything else, USER or not, gets its own bucket.
    String rateLimitActorUrn = null;
    boolean systemActor = false;
    if (authentication != null && authentication.getActor() != null) {
      String actorUrn = authentication.getActor().toUrnStr();
      String systemActorUrn = systemOperationContext.getAuthentication().getActor().toUrnStr();
      systemActor = actorUrn.equals(systemActorUrn);
      rateLimitActorUrn = systemActor ? null : actorUrn;
    }

    // Classify from the frontend-stamped header (advisory; trusted on the frontend-proxied hop, and
    // only applied when clientClassEnabled=true). Absent → NON_BROWSER.
    ClientClass clientClass =
        ClientClassifier.fromRequestSource(
            request.getHeader(ClientClassifier.REQUEST_SOURCE_HEADER));

    // Front gate: per-pod capacity + the scoped actor/class chain. Identity for unnamed queries is
    // the top-level field names (from the single analyze() above).
    RateLimitDecision rateLimitDecision =
        rateLimitEngine.evaluateAndAcquireGraphQL(
            request.getRequestURI(),
            request.getMethod(),
            documentMetadata.rateLimitIdentity(),
            rateLimitActorUrn,
            clientClass);
    if (!rateLimitDecision.isAllowed()) {
      return tooManyRequests(rateLimitDecision, mapper);
    }

    // Heavy-resolver gate (Part B): charge each configured heavy top-level resolver (reusing the
    // top-level fields from the single parse — no re-parse). On denial it releases the front-gate
    // lease before we reject.
    rateLimitDecision =
        applyHeavyResolverGate(
            rateLimitEngine,
            rateLimitDecision,
            documentMetadata.allRootFields(),
            systemActor,
            rateLimitActorUrn,
            clientClass);
    if (!rateLimitDecision.isAllowed()) {
      return tooManyRequests(rateLimitDecision, mapper);
    }

    SpringQueryContext context =
        new SpringQueryContext(
            true,
            authentication,
            _authorizerChain,
            systemOperationContext,
            configurationProvider,
            request,
            documentMetadata,
            variables,
            graphqlUsageClassificationRegistry);
    Span.current()
        .setAttribute(
            ACTOR_URN_ATTR,
            authentication.getActor() != null
                ? authentication.getActor().toUrnStr()
                : context.getActorUrn());

    final String threadName = Thread.currentThread().getName();
    final String queryName = context.getQueryName();
    log.debug("Query: {}, variables: {}", query, variables);

    final RateLimitLease rateLimitLease = rateLimitEngine.toLease(rateLimitDecision);
    final HttpHeaders rateLimitHeaders = new HttpHeaders();
    RateLimitHeaderWriter.createHeaders(rateLimitDecision).forEach(rateLimitHeaders::add);
    final AtomicBoolean executionSucceeded = new AtomicBoolean(false);
    final OperationContext usageSessionContext = context.getOperationContext();
    boolean asyncStarted = false;
    try {
      CompletableFuture<ResponseEntity<String>> executionFuture =
          GraphQLConcurrencyUtils.supplyAsync(
              () -> {
                log.debug("Executing operation {} for {}", queryName, threadName);

                /*
                 * Execute GraphQL Query
                 */
                ExecutionResult executionResult =
                    usageSessionContext.withSpan(
                        "graphql.execute",
                        () -> _engine.execute(query, operationName, variables, context),
                        "graphql.operation",
                        queryName == null ? "unknown" : queryName);

                executionSucceeded.set(executionResult.getErrors().isEmpty());

                if (!executionSucceeded.get()) {
                  // There were GraphQL errors. Report in error logs.
                  log.error(
                      "Errors while execu" + "" + "ting query: {}, result: {}, errors: {}",
                      StringUtils.abbreviate(query, MAX_LOG_WIDTH),
                      executionResult.toSpecification(),
                      executionResult.getErrors());
                }

                /*
                 * Format & Return Response
                 */
                try {
                  long totalDuration = submitMetrics(executionResult);
                  // Remove tracing from response to reduce bulk, not used by the frontend
                  executionResult.getExtensions().remove("tracing");
                  String responseBodyStr =
                      GRAPHQL_RESPONSE_MAPPER.writeValueAsString(executionResult.toSpecification());
                  if (totalDuration
                      >= configurationProvider.getGraphQL().getQuery().getSlowQueryThresholdMs()) {
                    log.info(
                        "Slow operation {} took {} ms (response size: {})",
                        queryName,
                        totalDuration,
                        responseBodyStr.length());
                  } else if (totalDuration > 0) {
                    log.debug(
                        "Executed operation {} in {} ms (response size: {})",
                        queryName,
                        totalDuration,
                        responseBodyStr.length());
                  } else {
                    log.debug(
                        "Executed operation {} (response size: {})",
                        queryName,
                        responseBodyStr.length());
                  }
                  log.trace("Execution result: {}", responseBodyStr);
                  if (usageMetricsSessionEnricher != null) {
                    usageMetricsSessionEnricher.recordResponseWithBytes(
                        usageSessionContext, (long) responseBodyStr.length());
                  }
                  return new ResponseEntity<>(responseBodyStr, rateLimitHeaders, HttpStatus.OK);
                } catch (IllegalArgumentException | JsonProcessingException e) {
                  log.error(
                      "Failed to convert execution result {} into a JsonNode",
                      executionResult.toSpecification());
                  return new ResponseEntity<>(rateLimitHeaders, HttpStatus.SERVICE_UNAVAILABLE);
                }
              },
              this.getClass().getSimpleName(),
              "postGraphQL");
      executionFuture.whenComplete(
          (response, error) ->
              rateLimitEngine.release(
                  rateLimitLease,
                  error == null
                      && response != null
                      && response.getStatusCode().is2xxSuccessful()
                      && executionSucceeded.get()));
      asyncStarted = true;
      return executionFuture;
    } finally {
      if (!asyncStarted) {
        rateLimitEngine.release(rateLimitLease, false);
      }
    }
  }

  @GetMapping("/graphql")
  void getGraphQL(HttpServletRequest request, HttpServletResponse response)
      throws HttpRequestMethodNotSupportedException {
    log.info("GET on GraphQL API is not supported");
    throw new HttpRequestMethodNotSupportedException("GET");
  }

  private void observeErrors(ExecutionResult executionResult) {
    executionResult
        .getErrors()
        .forEach(
            graphQLError -> {
              if (graphQLError instanceof DataHubGraphQLError) {
                DataHubGraphQLError dhGraphQLError = (DataHubGraphQLError) graphQLError;
                int errorCode = dhGraphQLError.getErrorCode();
                if (metricUtils != null)
                  metricUtils.increment(
                      MetricRegistry.name(
                          this.getClass(), "errorCode", Integer.toString(errorCode)),
                      1);
              } else {
                if (metricUtils != null)
                  metricUtils.increment(
                      MetricRegistry.name(
                          this.getClass(), "errorType", graphQLError.getErrorType().toString()),
                      1);
              }
            });
    if (executionResult.getErrors().size() != 0 && metricUtils != null) {
      metricUtils.increment(MetricRegistry.name(this.getClass(), "error"), 1);
    }
  }

  @SuppressWarnings("unchecked")
  private long submitMetrics(ExecutionResult executionResult) {
    try {
      observeErrors(executionResult);
      if (metricUtils != null)
        metricUtils.increment(MetricRegistry.name(this.getClass(), "call"), 1);
      Object tracingInstrumentation = executionResult.getExtensions().get("tracing");
      if (tracingInstrumentation instanceof Map) {
        Map<String, Object> tracingMap = (Map<String, Object>) tracingInstrumentation;
        long totalDuration = TimeUnit.NANOSECONDS.toMillis((long) tracingMap.get("duration"));
        Map<String, Object> executionData = (Map<String, Object>) tracingMap.get("execution");
        // Extract top level resolver, parent is top level query. Assumes single query per call.
        List<Map<String, Object>> resolvers =
            (List<Map<String, Object>>) executionData.get("resolvers");
        String fieldName =
            resolvers.stream()
                .filter(
                    resolver -> List.of("Query", "Mutation").contains(resolver.get("parentType")))
                .findFirst()
                .map(parentResolver -> parentResolver.get("fieldName"))
                .map(Object::toString)
                .orElse("UNKNOWN");
        if (metricUtils != null) metricUtils.histogram(this.getClass(), fieldName, totalDuration);
        return totalDuration;
      }
    } catch (Exception e) {
      if (metricUtils != null)
        metricUtils.increment(
            MetricRegistry.name(this.getClass(), "submitMetrics", "exception"), 1);
      log.error("Unable to submit metrics for GraphQL call.", e);
    }

    return -1;
  }
}
