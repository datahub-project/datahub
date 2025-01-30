package com.datahub.graphql;

import static com.linkedin.metadata.Constants.*;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.name.Named;
import com.linkedin.datahub.graphql.GraphQLEngine;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLError;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import graphql.ExecutionResult;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.api.trace.Span;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpEntity;
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

  public GraphQLController() {
    MetricUtils.get().counter(MetricRegistry.name(this.getClass(), "error"));
    MetricUtils.get().counter(MetricRegistry.name(this.getClass(), "call"));
  }

  @Inject GraphQLEngine _engine;

  @Inject AuthorizerChain _authorizerChain;

  @Inject ConfigurationProvider configurationProvider;

  @Nonnull
  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  private static final int MAX_LOG_WIDTH = 512;

  @PostMapping(value = "/graphql", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> postGraphQL(
      HttpServletRequest request, HttpEntity<String> httpEntity) {

    String jsonStr = httpEntity.getBody();
    ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
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

    /*
     * Extract "variables" map
     */
    JsonNode variablesJson = bodyJson.get("variables");
    final Map<String, Object> variables =
        (variablesJson != null && !variablesJson.isNull())
            ? new ObjectMapper()
                .convertValue(variablesJson, new TypeReference<Map<String, Object>>() {})
            : Collections.emptyMap();

    /*
     * Init QueryContext
     */
    Authentication authentication = AuthenticationContext.getAuthentication();

    SpringQueryContext context =
        new SpringQueryContext(
            true,
            authentication,
            _authorizerChain,
            systemOperationContext,
            configurationProvider,
            request,
            operationName,
            query,
            variables);
    Span.current().setAttribute("actor.urn", context.getActorUrn());

    final String threadName = Thread.currentThread().getName();
    final String queryName = context.getQueryName();
    log.debug("Query: {}, variables: {}", query, variables);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          log.info("Executing operation {} for {}", queryName, threadName);

          /*
           * Execute GraphQL Query
           */
          ExecutionResult executionResult =
              _engine.execute(query, operationName, variables, context);

          if (executionResult.getErrors().size() != 0) {
            // There were GraphQL errors. Report in error logs.
            log.error(
                "Errors while executing query: {}, result: {}, errors: {}",
                StringUtils.abbreviate(query, MAX_LOG_WIDTH),
                executionResult.toSpecification(),
                executionResult.getErrors());
          }

          /*
           * Format & Return Response
           */
          try {
            long totalDuration = submitMetrics(executionResult);
            String executionTook = totalDuration > 0 ? " in " + totalDuration + " ms" : "";
            log.info("Executed operation {}" + executionTook, queryName);
            // Remove tracing from response to reduce bulk, not used by the frontend
            executionResult.getExtensions().remove("tracing");
            String responseBodyStr =
                new ObjectMapper().writeValueAsString(executionResult.toSpecification());
            log.info("Operation {} execution result size: {}", queryName, responseBodyStr.length());
            log.trace("Execution result: {}", responseBodyStr);
            return new ResponseEntity<>(responseBodyStr, HttpStatus.OK);
          } catch (IllegalArgumentException | JsonProcessingException e) {
            log.error(
                "Failed to convert execution result {} into a JsonNode",
                executionResult.toSpecification());
            return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
          }
        },
        this.getClass().getSimpleName(),
        "postGraphQL");
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
                MetricUtils.get()
                    .counter(
                        MetricRegistry.name(
                            this.getClass(), "errorCode", Integer.toString(errorCode)))
                    .inc();
              } else {
                MetricUtils.get()
                    .counter(
                        MetricRegistry.name(
                            this.getClass(), "errorType", graphQLError.getErrorType().toString()))
                    .inc();
              }
            });
    if (executionResult.getErrors().size() != 0) {
      MetricUtils.get().counter(MetricRegistry.name(this.getClass(), "error")).inc();
    }
  }

  @SuppressWarnings("unchecked")
  private long submitMetrics(ExecutionResult executionResult) {
    try {
      observeErrors(executionResult);
      MetricUtils.get().counter(MetricRegistry.name(this.getClass(), "call")).inc();
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
        MetricUtils.get()
            .histogram(MetricRegistry.name(this.getClass(), fieldName))
            .update(totalDuration);
        return totalDuration;
      }
    } catch (Exception e) {
      MetricUtils.get()
          .counter(MetricRegistry.name(this.getClass(), "submitMetrics", "exception"))
          .inc();
      log.error("Unable to submit metrics for GraphQL call.", e);
    }

    return -1;
  }
}
