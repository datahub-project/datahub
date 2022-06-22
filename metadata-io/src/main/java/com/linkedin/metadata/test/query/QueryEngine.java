package com.linkedin.metadata.test.query;

import com.google.common.collect.Streams;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.definition.TestQuery;
import com.linkedin.metadata.test.definition.ValidationResult;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Engine for batch evaluating metadata test queries. It ties together internal evaluators to achieve it's goal
 */
public class QueryEngine {
  private final List<BaseQueryEvaluator> _queryEvaluators;

  public QueryEngine(List<BaseQueryEvaluator> queryEvaluators) {
    _queryEvaluators = queryEvaluators;
    queryEvaluators.forEach(evaluator -> evaluator.setQueryEngine(this));
  }

  // Batch evaluate a single query for the given entity urns
  public Map<Urn, TestQueryResponse> batchEvaluateQuery(Set<Urn> urns, TestQuery query) {
    return batchEvaluateQueries(urns, Collections.singleton(query)).entrySet()
        .stream()
        .filter(entry -> entry.getValue().containsKey(query))
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get(query)));
  }

  @WithSpan
  // Batch evaluate multiple queries for the given entity urns
  public Map<Urn, Map<TestQuery, TestQueryResponse>> batchEvaluateQueries(Collection<Urn> urns,
      Collection<TestQuery> queries) {
    queries.forEach(this::validateQuery);
    // First group urns by entity type - different entity types are eligible for different types of queries
    Map<String, Set<Urn>> urnsPerEntityType =
        urns.stream().collect(Collectors.groupingBy(Urn::getEntityType, Collectors.toSet()));
    Map<Urn, Map<TestQuery, TestQueryResponse>> finalResult = new HashMap<>();
    for (String entityType : urnsPerEntityType.keySet()) {
      Set<Urn> urnsOfType = urnsPerEntityType.get(entityType);

      // For each query, map it to the correct query evaluator
      List<Set<TestQuery>> queriesPerEvaluator =
          _queryEvaluators.stream().map(evaluator -> new HashSet<TestQuery>()).collect(Collectors.toList());
      for (TestQuery query : queries) {
        int eligibleEvaluatorIndex = -1;
        for (int i = 0; i < _queryEvaluators.size(); i++) {
          if (_queryEvaluators.get(i).isEligible(entityType, query)) {
            eligibleEvaluatorIndex = i;
            break;
          }
        }
        if (eligibleEvaluatorIndex < 0) {
          throw new UnsupportedOperationException(
              String.format("Unsupported query %s. No eligible query evaluator for the given query", query));
        }
        queriesPerEvaluator.get(eligibleEvaluatorIndex).add(query);
      }

      // Batch evaluate queries per evaluator
      List<Map<Urn, Map<TestQuery, TestQueryResponse>>> batchedResponse =
          Streams.zip(_queryEvaluators.stream(), queriesPerEvaluator.stream(), (evaluator, queryBatch) -> {
            if (queryBatch.isEmpty()) {
              return Collections.<Urn, Map<TestQuery, TestQueryResponse>>emptyMap();
            }
            return evaluator.evaluate(entityType, urnsOfType, queryBatch);
          }).collect(Collectors.toList());

      // Merge results back into finalResult
      for (Map<Urn, Map<TestQuery, TestQueryResponse>> responseFromEvaluator : batchedResponse) {
        responseFromEvaluator.forEach((urn, responses) -> {
          if (!finalResult.containsKey(urn)) {
            finalResult.put(urn, new HashMap<>());
          }
          finalResult.get(urn).putAll(responses);
        });
      }
    }
    return finalResult;
  }

  // Simple validation
  private void validateQuery(TestQuery query) {
    if (query.getQueryParts().isEmpty()) {
      throw new IllegalArgumentException("Input metadata test query is empty");
    }
  }

  // Validate whether the query is valid given the entity types
  // Find the correct evaluator eligible for the query and let the evaluator validate the query
  public ValidationResult validateQuery(TestQuery query, List<String> entityTypes) {
    List<String> messages = new ArrayList<>();
    for (String entityType : entityTypes) {
      Optional<BaseQueryEvaluator> eligibleEvaluator =
          _queryEvaluators.stream().filter(evaluator -> evaluator.isEligible(entityType, query)).findFirst();
      if (!eligibleEvaluator.isPresent()) {
        messages.add(String.format(
            "Query %s is invalid for entity type %s: No eligible query evaluator found. Make sure the %s is a valid aspect",
            query, entityType, query.getQueryParts().get(0)));
        continue;
      }
      ValidationResult validationResult = eligibleEvaluator.get().validateQuery(entityType, query);
      if (validationResult.isValid()) {
        return ValidationResult.validResult();
      }
      messages.addAll(validationResult.getMessages());
    }
    return new ValidationResult(false, messages);
  }
}
