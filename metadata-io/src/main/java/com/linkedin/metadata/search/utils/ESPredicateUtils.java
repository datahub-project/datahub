package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.ESUtils.*;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.DateLiteral;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.query.TestQuery;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;

@Slf4j
public class ESPredicateUtils {
  private ESPredicateUtils() {}

  @Nonnull
  public static BoolQueryBuilder applyDefaultSearchFilters(
      @Nonnull OperationContext opContext,
      @Nullable Predicate predicate,
      @Nonnull BoolQueryBuilder filterQuery) {
    // filter soft deleted entities by default
    filterSoftDeletedByDefault(
        predicate, filterQuery, opContext.getSearchContext().getSearchFlags());
    // Saas Only!
    // filter based on access controls
    ESAccessControlUtil.buildAccessControlFilters(opContext).ifPresent(filterQuery::filter);
    return filterQuery;
  }

  public static void filterSoftDeletedByDefault(
      @Nullable Predicate predicate,
      @Nonnull BoolQueryBuilder filterQuery,
      SearchFlags searchFlags) {
    if (Boolean.FALSE.equals(searchFlags.isIncludeSoftDeleted())) {
      boolean removedInOrFilter = false;
      if (predicate != null) {
        removedInOrFilter =
            Predicate.extractQueriesForPredicate(predicate).stream()
                .anyMatch(
                    testQuery ->
                        REMOVED.equals(testQuery.getQuery())
                            || (REMOVED + KEYWORD_SUFFIX).equals(testQuery.getQuery()));
      }
      if (!removedInOrFilter) {
        filterQuery.mustNot(QueryBuilders.matchQuery(REMOVED, true));
      }
    }
  }

  public static BoolQueryBuilder buildFilterQuery(
      @Nullable Predicate predicate,
      boolean isTimeSeries,
      final Map<PathSpec, String> searchableFieldPaths,
      final Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      OperationContext opContext) {
    BoolQueryBuilder finalQueryBuilder = QueryBuilders.boolQuery();
    if (predicate == null) {
      return finalQueryBuilder;
    }

    ESPredicateUtils.validatePredicate(
        predicate, opContext.getRetrieverContext().get().getAspectRetriever());
    processPredicate(
        predicate, finalQueryBuilder, isTimeSeries, searchableFieldTypes, searchableFieldPaths);
    return finalQueryBuilder;
  }

  public static void validatePredicate(
      @Nullable Predicate predicate, @Nonnull AspectRetriever aspectRetriever) {

    if (predicate == null) {
      return;
    }

    Set<String> fieldNames = new HashSet<>();
    Set<TestQuery> queries = Predicate.extractQueriesForPredicate(predicate);
    queries.stream()
        .filter(
            testQuery -> testQuery.getQuery().startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX))
        .forEach(
            testQuery ->
                fieldNames.add(
                    testQuery
                        .getQuery()
                        .substring(STRUCTURED_PROPERTY_MAPPING_FIELD.length() + 1)));

    if (!fieldNames.isEmpty()) {
      StructuredPropertyUtils.validateStructuredPropertyFQN(fieldNames, aspectRetriever);
    }
  }

  private static void processPredicate(
      Predicate predicate,
      final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    OperatorType operatorType = predicate.getOperatorType();
    switch (operatorType) {
      case OR:
        evaluateOrPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case AND:
        evaluateAndPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case ANY_EQUALS:
        evaluateAnyEqualsPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case STARTS_WITH:
        evaluateStartsWithPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case CONTAINS_ANY:
        evaluateContainsAnyPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case CONTAINS_STR:
        evaluateContainsStringPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case REGEX_MATCH:
        evaluateRegexMatchPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case GREATER_THAN:
        evaluateGreaterThanPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case LESS_THAN:
        evaluateLessThanPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case EXISTS:
        evaluateExistsPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case IS_FALSE:
        evaluateIsFalsePredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case IS_TRUE:
        evaluateIsTruePredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case NOT:
        evaluateNotPredicate(
            predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
        break;
      case QUERY:
        evaluateQueryPredicate(predicate);
        break;
      default:
        throw new IllegalArgumentException("Invalid OperatorType: " + operatorType);
    }
  }

  // And predicates and Or predicates always have subpredicates as all operands. May be be 1 or more
  private static void evaluateOrPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(
        operand ->
            processSubQuery(
                queryBuilder, operand, isTimeseries, searchableFieldTypes, searchableFieldPaths));
    queryBuilder.minimumShouldMatch(1);
  }

  private static void evaluateAndPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(
        operand ->
            processSubQuery(
                queryBuilder, operand, isTimeseries, searchableFieldTypes, searchableFieldPaths));
    queryBuilder.minimumShouldMatch(operands.size());
  }

  // Recurse down predicate tree
  private static void processSubQuery(
      @Nonnull final BoolQueryBuilder queryBuilder,
      @Nonnull final Operand operand,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
    processPredicate(
        (Predicate) operand.getExpression(),
        subQueryBuilder,
        isTimeseries,
        searchableFieldTypes,
        searchableFieldPaths);
    queryBuilder.should(subQueryBuilder);
  }

  // Any Equals must be a Query followed by a StringListLiteral of values to match the query value
  // to,
  // always has exactly 2 ops
  private static void evaluateAnyEqualsPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.operands().get();
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    TermsQueryBuilder termsQueryBuilder =
        QueryBuilders.termsQuery(
            finalFieldName,
            values.stream()
                .map(value -> resolveFieldValue(resolvedFieldTypes, value))
                .collect(Collectors.toList()));
    queryBuilder.should(termsQueryBuilder);
    queryBuilder.minimumShouldMatch(1);
  }

  public static StringArray getSearchValueField(Operand valueOperand) {
    StringArray valuesArray = new StringArray();
    if (valueOperand.getExpression() instanceof StringListLiteral) {
      StringListLiteral stringListLiteral = (StringListLiteral) valueOperand.getExpression();
      valuesArray.addAll(stringListLiteral.getValues());
      return valuesArray;
    } else if (valueOperand.getExpression() instanceof DateLiteral) {
      DateLiteral dateLiteral = (DateLiteral) valueOperand.getExpression();
      valuesArray.add(dateLiteral.resolveValue());
      return valuesArray;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported value type: " + valueOperand.getExpression().getClass().getName());
    }
  }

  @Nonnull
  private static String resolveField(Query query, Map<PathSpec, String> fieldPaths) {
    String fieldName = fieldPaths.get(new PathSpec(query.getQuery().getQueryParts()));
    if (fieldName == null) {
      throw new IllegalArgumentException("Unable to find field path for query: " + query);
    }
    return fieldName;
  }

  private static Set<String> resolveFieldTypes(
      String fieldName, Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes) {
    Set<SearchableAnnotation.FieldType> resolvedFieldTypes =
        fieldTypes.getOrDefault(fieldName, Collections.emptySet());
    Set<String> finalFieldTypes =
        resolvedFieldTypes.stream()
            .map(ESUtils::getElasticTypeForFieldType)
            .collect(Collectors.toSet());
    if (finalFieldTypes.size() > 1) {
      log.warn(
          "Multiple field types for field name {}, determining best fit for set: {}",
          fieldName,
          finalFieldTypes);
    }
    return finalFieldTypes;
  }

  private static Object resolveFieldValue(Set<String> finalFieldTypes, String fieldValue) {
    if (finalFieldTypes.contains(BOOLEAN_FIELD_TYPE)) {
      return Boolean.parseBoolean(fieldValue);
    } else if (finalFieldTypes.contains(LONG_FIELD_TYPE)
        || finalFieldTypes.contains(DATE_FIELD_TYPE)) {
      return Long.parseLong(fieldValue);
    } else if (finalFieldTypes.contains(DOUBLE_FIELD_TYPE)) {
      return Double.parseDouble(fieldValue);
    }
    return fieldValue;
  }

  private static String determineKeyword(
      String fieldName, Set<String> fieldTypes, boolean isTimeseries) {
    if (fieldTypes.contains(BOOLEAN_FIELD_TYPE)
        || fieldTypes.contains(LONG_FIELD_TYPE)
        || fieldTypes.contains(DATE_FIELD_TYPE)
        || fieldTypes.contains(DOUBLE_FIELD_TYPE)) {
      return fieldName;
    }
    return toKeywordField(fieldName, isTimeseries);
  }

  // Starts With must be a Query followed by a StringListLiteral of values to match the query value
  // to,
  // always has exactly 2 ops
  private static void evaluateStartsWithPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();
    // Starts With must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    queryBuilder.minimumShouldMatch(1);
    // We don't resolve values here, must be treated as a string
    values.forEach(value -> queryBuilder.should(QueryBuilders.prefixQuery(finalFieldName, value)));
  }

  // TODO: Evaluators for these two are equivalent and could be condensed, note that the naming of
  // this evaluator likely
  //      causes confusion as it does not do the same thing as contains string
  private static void evaluateContainsAnyPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    evaluateAnyEqualsPredicate(
        predicate, queryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
  }

  // Contains String must be a Query followed by a StringListLiteral of values to check if the
  // queried field contains
  // any of, always has exactly 2 ops. Note: Can be a relatively expensive query to perform and
  // should be used sparingly
  private static void evaluateContainsStringPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();
    // Contains String must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    queryBuilder.minimumShouldMatch(1);
    // We don't resolve values here, must be treated as string
    values.forEach(
        value -> QueryBuilders.queryStringQuery("*" + value + "*").field(finalFieldName));
  }

  // Regex match must be a Query followed by a StringListLiteral of regex strings to match against,
  // always has exactly 2 ops. Note: Can be a relatively expensive query to perform and should be
  // used sparingly
  private static void evaluateRegexMatchPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();
    // Regex Match must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    queryBuilder.minimumShouldMatch(1);
    // We don't resolve values here, must be treated as a string
    values.forEach(value -> QueryBuilders.regexpQuery(finalFieldName, value));
  }

  // Greater Than must be a query followed by a StringListLiteral of values to evaluate a greater
  // than inequality against,
  // always has exactly 2 ops.
  private static void evaluateGreaterThanPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {

    List<Operand> operands = predicate.getOperands().get();
    // Greater Than must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    queryBuilder.minimumShouldMatch(1);
    values.forEach(
        value ->
            QueryBuilders.rangeQuery(finalFieldName)
                .gt(resolveFieldValue(resolvedFieldTypes, value)));
  }

  // Less Than must be a query followed by a StringListLiteral of values to evaluate a greater than
  // inequality against,
  // always has exactly 2 ops.
  private static void evaluateLessThanPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {

    List<Operand> operands = predicate.getOperands().get();
    // Greater Than must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    StringArray values = getSearchValueField(operands.get(1));
    queryBuilder.minimumShouldMatch(1);
    values.forEach(
        value ->
            QueryBuilders.rangeQuery(finalFieldName)
                .lt(resolveFieldValue(resolvedFieldTypes, value)));
  }

  // Exists must be a Query, always has exactly 1 op
  private static void evaluateExistsPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    queryBuilder.must(QueryBuilders.existsQuery(finalFieldName));
  }

  // Is False must be a Query, always has exactly 1 op
  private static void evaluateIsFalsePredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    queryBuilder.must(QueryBuilders.termQuery(finalFieldName, Boolean.FALSE));
  }

  // Is True must be a Query, always has exactly 1 op
  private static void evaluateIsTruePredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    String resolvedField = resolveField(query, searchableFieldPaths);
    Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
    String finalFieldName = determineKeyword(resolvedField, resolvedFieldTypes, isTimeseries);
    queryBuilder.must(QueryBuilders.termQuery(finalFieldName, Boolean.TRUE));
  }

  private static void evaluateNotPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths) {
    List<Operand> operands = predicate.getOperands().get();
    // Not always has one predicate type op
    Predicate subPredicate = (Predicate) operands.get(0).getExpression();
    BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
    processPredicate(
        subPredicate, subQueryBuilder, isTimeseries, searchableFieldTypes, searchableFieldPaths);
    queryBuilder.mustNot(subQueryBuilder);
  }

  private static void evaluateQueryPredicate(@Nonnull final Predicate predicate) {
    throw new IllegalArgumentException(
        "Query not allowed as a top level predicate, should be applied to a particular "
            + "operation: "
            + predicate);
  }
}
