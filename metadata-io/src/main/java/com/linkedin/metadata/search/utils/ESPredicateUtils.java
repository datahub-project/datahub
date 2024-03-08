package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;


public class ESPredicateUtils {
  private ESPredicateUtils() {}

  private static final Map<String, String> QUERY_SEARCH_FIELD_MAP =
      Map.of(
          "dataPlatformInstance.platform", "platform",
          "subTypes.typeNames", "typeNames",
          "urn", "urn.keyword",
          "globalTags.tags.tag", "tags",
          "glossaryTerms.terms.urn", "glossaryTerms",
          //      "glossaryTerms.terms.urn.glossaryTermInfo.parentNode", null,
          "domains.domains", "domains",
          "ownership.owners.owner", "owners",
          "container.container", "container",
          "operation.lastUpdatedTimestamp", "lastOperationTime");

  public static BoolQueryBuilder filterSoftDeletedByDefault(
      @Nullable Predicate predicate, @Nullable BoolQueryBuilder filterQuery) {
    boolean removedInOrFilter = false;
    if (predicate != null) {
      removedInOrFilter =
          filter.getOr().stream()
              .anyMatch(
                  or ->
                      or.getAnd().stream()
                          .anyMatch(
                              criterion ->
                                  criterion.getField().equals(REMOVED)
                                      || criterion.getField().equals(REMOVED + KEYWORD_SUFFIX)));
    }
    if (!removedInOrFilter) {
      filterQuery.mustNot(QueryBuilders.matchQuery(REMOVED, true));
    }
    return filterQuery;
  }

  public static BoolQueryBuilder buildFilterQuery(@Nullable Predicate predicate, boolean isTimeSeries,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes) {
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    if (predicate == null) {
      return finalQueryBuilder;
    }
    if (!predicate.getOperands().get().isEmpty()) {
      // TODO: validate predicate before processing
      processPredicate(predicate, finalQueryBuilder);
    }
    return finalQueryBuilder;
  }

  private static void processPredicate(Predicate predicate, final BoolQueryBuilder queryBuilder) {
    OperatorType operatorType = predicate.getOperatorType();
    switch (operatorType) {
      case OR:
        evaluateOrPredicate(predicate, queryBuilder);
        break;
      case AND:
        evaluateAndPredicate(predicate, queryBuilder);
        break;
      case ANY_EQUALS:
        evaluateAnyEqualsPredicate(predicate, queryBuilder);
        break;
      case STARTS_WITH:
        evaluateStartsWithPredicate(predicate, queryBuilder);
        break;
      case CONTAINS_ANY:
        evaluateContainsAnyPredicate(predicate, queryBuilder);
        break;
      case CONTAINS_STR:
        evaluateContainsStringPredicate(predicate, queryBuilder);
        break;
      case REGEX_MATCH:
        evaluateRegexMatchPredicate(predicate, queryBuilder);
        break;
      case GREATER_THAN:
        evaluateGreaterThanPredicate(predicate, queryBuilder);
        break;
      case LESS_THAN:
        evaluateLessThanPredicate(predicate, queryBuilder);
        break;
      case EXISTS:
        evaluateExistsPredicate(predicate, queryBuilder);
        break;
      case IS_FALSE:
        evaluateIsFalsePredicate(predicate, queryBuilder);
        break;
      case IS_TRUE:
        evaluateIsTruePredicate(predicate, queryBuilder);
        break;
      case NOT:
        evaluateNotPredicate(predicate, queryBuilder);
        break;
      case QUERY:
        evaluateQueryPredicate(predicate, queryBuilder);
        break;
      default:
        throw new IllegalArgumentException("Invalid OperatorType: " + operatorType);
    }
  }

  // And predicates and Or predicates always have subpredicates as all operands. Maybe be 1 or more
  private static void evaluateOrPredicate(@Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(operand -> processSubQuery(queryBuilder, operand));
    queryBuilder.minimumShouldMatch(1);
  }

  private static void evaluateAndPredicate(@Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(operand -> processSubQuery(queryBuilder, operand));
    queryBuilder.minimumShouldMatch(operands.size());
  }

  // Recurse down predicate tree
  private static void processSubQuery(@Nonnull final BoolQueryBuilder queryBuilder, @Nonnull final Operand operand) {
    BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
    processPredicate((Predicate) operand.getExpression(), subQueryBuilder);
    queryBuilder.should(subQueryBuilder);
  }

  // Any Equals must be a Query followed by a StringListLiteral of values to match the query value to,
  // always has exactly 2 ops
  private static void evaluateAnyEqualsPredicate(@Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder) {
    List<Operand> operands = predicate.operands().get();
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = QUERY_SEARCH_FIELD_MAP.get(query.getQuery().getQuery());
    if (resolvedField == null) {
      throw new UnsupportedOperationException("Could not resolve field for query: " + query.getQuery().getQuery());
    }
    StringListLiteral values = (StringListLiteral) operands.get(1).getExpression();
    TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(resolvedField, values.getValues());
    queryBuilder.should(termsQueryBuilder);
    queryBuilder.minimumShouldMatch(1);
  }

  // Starts With must be a Query followed by a StringListLiteral of values to match the query value to,
  // always has exactly 2 ops
  private static void evaluateStartsWithPredicate(@Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder) {

  }
}
