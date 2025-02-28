package com.linkedin.metadata.test.query.virtualFields;

import static com.linkedin.metadata.Constants.*;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.BaseQueryEvaluator;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Evaluator that supports resolving the ElasticSearch virtual fields '_entityType' and '_urn' as
 * query values
 */
@Slf4j
@RequiredArgsConstructor
public class VirtualFieldsQueryEvaluator extends BaseQueryEvaluator {

  @Override
  public boolean isEligible(@Nonnull final String entityType, @Nonnull final TestQuery query) {
    return isVirtualFieldsQuery(query);
  }

  @Override
  @Nonnull
  public ValidationResult validateQuery(
      @Nonnull final String entityType, @Nonnull final TestQuery query)
      throws IllegalArgumentException {
    return new ValidationResult(isEligible(entityType, query), Collections.emptyList());
  }

  @Override
  @Nonnull
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(
      @Nonnull OperationContext opContext,
      @Nonnull final String entityType,
      @Nonnull final Set<Urn> urns,
      @Nonnull final Set<TestQuery> queries) {
    final Map<Urn, Map<TestQuery, TestQueryResponse>> result = new HashMap<>();
    for (TestQuery query : queries) {
      for (Urn urn : urns) {
        result.putIfAbsent(urn, new HashMap<>());
        switch (query.getQuery()) {
          case SearchUtil.INDEX_VIRTUAL_FIELD:
            // Forms tests can generate with either UPPER_SNAKE_CASE or lowerCamelCase, returns the
            // values for both
            String upperSnake =
                CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, urn.getEntityType());
            result
                .get(urn)
                .put(
                    query,
                    new TestQueryResponse(ImmutableList.of(upperSnake, urn.getEntityType())));
            break;
          case SearchUtil.URN_FIELD:
            result
                .get(urn)
                .put(query, new TestQueryResponse(Collections.singletonList(urn.toString())));
            break;
          default:
            // Should never happen
            throw new IllegalArgumentException(
                "Invalid query: " + query + " cannot be evaluated by this evaluator.");
        }
      }
    }
    return result;
  }

  private boolean isVirtualFieldsQuery(@Nonnull final TestQuery query) {
    return SearchUtil.INDEX_VIRTUAL_FIELD.equals(query.getQuery())
        || SearchUtil.URN_FIELD.equals(query.getQuery());
  }
}
