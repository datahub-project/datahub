package com.linkedin.metadata.test.query;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.definition.ValidationResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Base interface for query evaluators, which defines the set of queries that are eligible for this
 * evaluator and the evaluation logic
 *
 * <p>TODO: Rebrand this as PropertyResolver. And simplify the API.
 */
public interface QueryEvaluator {
  /** Get the query engine to recursively evaluate query */
  QueryEngine getQueryEngine();

  /** Set the query engine to recursively evaluate query */
  void setQueryEngine(QueryEngine queryEngine);

  /** Whether the query is eligible for this evaluator */
  boolean isEligible(String entityType, TestQuery query);

  /** Validate the query given the entity type. Throw IllegalArgumentException if not valid. */
  ValidationResult validateQuery(String entityType, TestQuery query)
      throws IllegalArgumentException;

  /** Evaluate the input queries for the given urn. Note all urns must be of type entityType */
  Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(
      @Nonnull OperationContext opContext,
      String entityType,
      Set<Urn> urns,
      Set<TestQuery> queries);
}
