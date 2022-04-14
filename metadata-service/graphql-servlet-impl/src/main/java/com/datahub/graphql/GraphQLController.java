package com.datahub.graphql;

import com.datahub.authentication.AuthenticationContext;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.GraphQLEngine;
import graphql.ExecutionResult;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RestController
public class GraphQLController {

  public GraphQLController() {

  }

  @Inject
  GraphQLEngine _engine;

  @Inject
  AuthorizerChain _authorizerChain;

  @PostMapping(value = "/graphql", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> postGraphQL(HttpEntity<String> httpEntity) {

    String jsonStr = httpEntity.getBody();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode bodyJson = null;
    try {
      bodyJson = mapper.readTree(jsonStr);
    } catch (JsonProcessingException e) {
      log.error(String.format("Failed to parse json %s", jsonStr));
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

    /*
     * Extract "variables" map
     */
    JsonNode variablesJson = bodyJson.get("variables");
    final Map<String, Object> variables = (variablesJson != null && !variablesJson.isNull())
      ? new ObjectMapper().convertValue(variablesJson, new TypeReference<Map<String, Object>>() { })
      : Collections.emptyMap();

    log.debug(String.format("Executing graphQL query: %s, variables: %s", queryJson, variables));

    /*
     * Init QueryContext
     */
    Authentication authentication = AuthenticationContext.getAuthentication();
    SpringQueryContext context = new SpringQueryContext(
        true,
        authentication,
        _authorizerChain);

    return CompletableFuture.supplyAsync(() -> {
      /*
       * Execute GraphQL Query
       */
      ExecutionResult executionResult = _engine.execute(queryJson.asText(), variables, context);

      if (executionResult.getErrors().size() != 0) {
        // There were GraphQL errors. Report in error logs.
        log.error(String.format("Errors while executing graphQL query: %s, result: %s, errors: %s",
            queryJson,
            executionResult.toSpecification(),
            executionResult.getErrors()));
      } else {
        log.debug(String.format("Executed graphQL query: %s, result: %s",
            queryJson,
            executionResult.toSpecification()));
      }

      /*
       * Format & Return Response
       */
      try {
        String responseBodyStr = new ObjectMapper().writeValueAsString(executionResult.toSpecification());
        return new ResponseEntity<>(responseBodyStr, HttpStatus.OK);
      } catch (IllegalArgumentException | JsonProcessingException e) {
        log.error(String.format("Failed to convert execution result %s into a JsonNode", executionResult.toSpecification()));
        return new ResponseEntity<>(HttpStatus.SERVICE_UNAVAILABLE);
      }
    });
  }

  @GetMapping("/graphql")
  void getGraphQL(HttpServletRequest request, HttpServletResponse response) {
    throw new UnsupportedOperationException("GraphQL gets not supported.");
  }
}
