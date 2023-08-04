package io.datahub.test.query;

import com.linkedin.common.urn.Urn;
import io.datahub.test.definition.ValidationResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;


/**
 * Evaluator that supports resolving the 'entityType' and
 * 'urn' queries for a given URN.
 */
@RequiredArgsConstructor
public class EntityUrnTypeEvaluator extends BaseQueryEvaluator {

  private static final String ENTITY_TYPE_FIELD_NAME = "entityType";
  private static final String URN_FIELD_NAME = "urn";

  @Override
  public boolean isEligible(String entityType, TestQuery query) {
    if (query.getQueryParts().size() != 1) {
      return false;
    }
    final String queryName = query.getQuery();
    return ENTITY_TYPE_FIELD_NAME.equalsIgnoreCase(queryName) || URN_FIELD_NAME.equalsIgnoreCase(queryName);
  }

  @Override
  public ValidationResult validateQuery(String entityType, TestQuery query) throws IllegalArgumentException {
    boolean res = query.getQueryParts().size() == 1
        && (ENTITY_TYPE_FIELD_NAME.equalsIgnoreCase(query.getQuery()) || URN_FIELD_NAME.equalsIgnoreCase(query.getQuery()));
    return new ValidationResult(res, Collections.emptyList());
  }

  @Override
  @Nonnull
  public Map<Urn, Map<TestQuery, TestQueryResponse>> evaluate(
      @Nonnull String entityType,
      @Nonnull Set<Urn> urns,
      @Nonnull Set<TestQuery> queries) {
    final Map<Urn, Map<TestQuery, TestQueryResponse>> result = new HashMap<>();
    for (TestQuery query : queries) {
      for (Urn urn : urns) {
        result.putIfAbsent(urn, new HashMap<>());
        result.get(urn).put(query, buildUrnTypeQueryResponse(urn, query));
      }
    }
    return result;
  }

  private TestQueryResponse buildUrnTypeQueryResponse(@Nonnull Urn urn, @Nonnull TestQuery query) {
    final String queryName = query.getQuery();
    return new TestQueryResponse(Collections.singletonList(
        URN_FIELD_NAME.equalsIgnoreCase(queryName)
          ? urn.toString() // Query is 'urn'
          : urn.getEntityType() // Query is 'entityType'
    ));
  }
}
