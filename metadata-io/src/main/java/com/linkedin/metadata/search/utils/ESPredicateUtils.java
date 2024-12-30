package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.ESUtils.*;
import static com.linkedin.metadata.utils.SearchUtil.*;

import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.test.definition.expression.Expression;
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
import java.util.Optional;
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
        predicate, opContext.getRetrieverContext().getAspectRetriever());
    processPredicate(
        predicate,
        finalQueryBuilder,
        isTimeSeries,
        searchableFieldTypes,
        searchableFieldPaths,
        opContext);
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
      @Nonnull Predicate predicate,
      final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    OperatorType operatorType = predicate.getOperatorType();
    switch (operatorType) {
      case OR:
        evaluateOrPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case AND:
        evaluateAndPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case ANY_EQUALS:
        evaluateAnyEqualsPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case STARTS_WITH:
        evaluateStartsWithPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case CONTAINS_ANY:
        evaluateContainsAnyPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case CONTAINS_STR:
        evaluateContainsStringPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case REGEX_MATCH:
        evaluateRegexMatchPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case GREATER_THAN:
        evaluateGreaterThanPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case LESS_THAN:
        evaluateLessThanPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case EXISTS:
        evaluateExistsPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case IS_FALSE:
        evaluateIsFalsePredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case IS_TRUE:
        evaluateIsTruePredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
        break;
      case NOT:
        evaluateNotPredicate(
            predicate,
            queryBuilder,
            isTimeseries,
            searchableFieldTypes,
            searchableFieldPaths,
            opContext);
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
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(
        operand ->
            processSubQuery(
                queryBuilder,
                operand,
                isTimeseries,
                searchableFieldTypes,
                searchableFieldPaths,
                opContext));
    queryBuilder.minimumShouldMatch(1);
  }

  private static void evaluateAndPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.operands().get();
    operands.forEach(
        operand ->
            processSubQuery(
                queryBuilder,
                operand,
                isTimeseries,
                searchableFieldTypes,
                searchableFieldPaths,
                opContext));
    queryBuilder.minimumShouldMatch(operands.size());
  }

  // Recurse down predicate tree
  private static void processSubQuery(
      @Nonnull final BoolQueryBuilder queryBuilder,
      @Nonnull final Operand operand,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
    processPredicate(
        (Predicate) operand.getExpression(),
        subQueryBuilder,
        isTimeseries,
        searchableFieldTypes,
        searchableFieldPaths,
        opContext);
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
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.operands().get();
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      TermsQueryBuilder termsQueryBuilder =
          QueryBuilders.termsQuery(
              finalFieldName,
              values.stream()
                  .map(value -> resolveFieldValue(resolvedFieldTypes, value))
                  .collect(Collectors.toList()));
      queryBuilder.should(termsQueryBuilder);
    }
    queryBuilder.minimumShouldMatch(1);
  }

  public static StringArray getSearchValueField(
      Expression valueExpression, String fieldName, OperationContext opContext) {
    StringArray valuesArray = new StringArray();
    if (valueExpression instanceof StringListLiteral) {
      StringListLiteral stringListLiteral = (StringListLiteral) valueExpression;
      List<String> values;
      if (ES_INDEX_FIELD.equals(fieldName)) {
        values =
            stringListLiteral.getValues().stream()
                .map(opContext.getSearchContext().getIndexConvention()::getEntityIndexName)
                .collect(Collectors.toList());
      } else {
        values = stringListLiteral.getValues();
      }
      valuesArray.addAll(values);
      return valuesArray;
    } else if (valueExpression instanceof DateLiteral) {
      DateLiteral dateLiteral = (DateLiteral) valueExpression;
      valuesArray.add(dateLiteral.resolveValue());
      return valuesArray;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported value type: " + valueExpression.getClass().getName());
    }
  }

  @Nonnull
  public static List<String> resolveField(
      Query query,
      Map<PathSpec, String> fieldPaths,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      AspectRetriever aspectRetriever) {
    List<String> queryParts = query.getQuery().getQueryParts();
    // Handle virtual field mapping
    if (queryParts.size() == 1 && INDEX_VIRTUAL_FIELD.equals(queryParts.get(0))) {
      return Collections.singletonList(ES_INDEX_FIELD);
    }
    // Handle urn mapping
    if (queryParts.size() == 1 && URN_FIELD.equals(queryParts.get(0))) {
      return Collections.singletonList(URN_FIELD);
    }
    // If field is already resolved to a searchable field, just use it
    if (searchableFieldTypes.containsKey(query.getQuery().getQuery())) {
      String queryFieldName = query.getQuery().getQuery();
      final Optional<List<String>> maybeFieldToExpand =
          Optional.ofNullable(FIELDS_TO_EXPANDED_FIELDS_LIST.get(queryFieldName));
      return maybeFieldToExpand.orElseGet(() -> Collections.singletonList(queryFieldName));
    }

    // Handle structured properties
    if (STRUCTURED_PROPERTY_MAPPING_FIELD.equals(queryParts.get(0))) {
      return Collections.singletonList(
          StructuredPropertyUtils.lookupDefinitionFromFilterOrFacetName(
                  query.getQuery().getQuery(), aspectRetriever)
              .map(
                  urnDefinition ->
                      STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                          + StructuredPropertyUtils.toElasticsearchFieldName(
                              urnDefinition.getFirst(), urnDefinition.getSecond()))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Unable to find field path for query: " + query)));
    }
    // Try to resolve by path
    String fieldName = fieldPaths.get(new PathSpec(queryParts));
    if (fieldName == null) {
      throw new IllegalArgumentException("Unable to find field path for query: " + query);
    }
    final Optional<List<String>> maybeFieldToExpand =
        Optional.ofNullable(FIELDS_TO_EXPANDED_FIELDS_LIST.get(fieldName));
    return maybeFieldToExpand.orElseGet(() -> Collections.singletonList(fieldName));
  }

  public static Set<String> resolveFieldTypes(
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

  public static String determineKeyword(
      String fieldName,
      Set<String> fieldTypes,
      boolean isTimeseries,
      @Nullable AspectRetriever aspectRetriever) {
    if (fieldTypes.contains(BOOLEAN_FIELD_TYPE)
        || fieldTypes.contains(LONG_FIELD_TYPE)
        || fieldTypes.contains(DATE_FIELD_TYPE)
        || fieldTypes.contains(DOUBLE_FIELD_TYPE)
        || fieldTypes.contains(OBJECT_FIELD_TYPE)) {
      return fieldName;
    }
    return toKeywordField(fieldName, isTimeseries, aspectRetriever);
  }

  // Starts With must be a Query followed by a StringListLiteral of values to match the query value
  // to,
  // always has exactly 2 ops
  private static void evaluateStartsWithPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();
    // Starts With must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      // We don't resolve values here, must be treated as a string
      values.forEach(
          value -> queryBuilder.should(QueryBuilders.prefixQuery(finalFieldName, value)));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // TODO: Evaluators for these two are equivalent and could be condensed, note that the naming of
  // this evaluator likely
  //      causes confusion as it does not do the same thing as contains string
  private static void evaluateContainsAnyPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    evaluateAnyEqualsPredicate(
        predicate,
        queryBuilder,
        isTimeseries,
        searchableFieldTypes,
        searchableFieldPaths,
        opContext);
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
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();
    // Contains String must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      // We don't resolve values here, must be treated as string
      values.forEach(
          value ->
              queryBuilder.should(
                  QueryBuilders.queryStringQuery("*" + value + "*").field(finalFieldName)));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // Regex match must be a Query followed by a StringListLiteral of regex strings to match against,
  // always has exactly 2 ops. Note: Can be a relatively expensive query to perform and should be
  // used sparingly
  private static void evaluateRegexMatchPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();
    // Regex Match must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      // We don't resolve values here, must be treated as a string
      values.forEach(
          value -> queryBuilder.should(QueryBuilders.regexpQuery(finalFieldName, value)));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // Greater Than must be a query followed by a StringListLiteral of values to evaluate a greater
  // than inequality against,
  // always has exactly 2 ops.
  private static void evaluateGreaterThanPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {

    List<Operand> operands = predicate.getOperands().get();
    // Greater Than must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      values.forEach(
          value ->
              queryBuilder.should(
                  QueryBuilders.rangeQuery(finalFieldName)
                      .gt(resolveFieldValue(resolvedFieldTypes, value))));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // Less Than must be a query followed by a StringListLiteral of values to evaluate a greater than
  // inequality against,
  // always has exactly 2 ops.
  private static void evaluateLessThanPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {

    List<Operand> operands = predicate.getOperands().get();
    // Greater Than must be a Query followed by a StringListLiteral of values to match the query
    // value to, always has exactly 2 ops
    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      StringArray values =
          getSearchValueField(operands.get(1).getExpression(), finalFieldName, opContext);
      values.forEach(
          value ->
              queryBuilder.should(
                  QueryBuilders.rangeQuery(finalFieldName)
                      .lt(resolveFieldValue(resolvedFieldTypes, value))));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // Exists must be a Query, always has exactly 1 op
  private static void evaluateExistsPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      queryBuilder.should(QueryBuilders.existsQuery(finalFieldName));
    }

    queryBuilder.minimumShouldMatch(1);
  }

  // Is False must be a Query, always has exactly 1 op
  private static void evaluateIsFalsePredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      queryBuilder.should(QueryBuilders.termQuery(finalFieldName, Boolean.FALSE));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  // Is True must be a Query, always has exactly 1 op
  private static void evaluateIsTruePredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();

    Query query = (Query) operands.get(0).getExpression();
    List<String> resolvedFields =
        resolveField(
            query, searchableFieldPaths, searchableFieldTypes, opContext.getAspectRetriever());
    for (String resolvedField : resolvedFields) {
      Set<String> resolvedFieldTypes = resolveFieldTypes(resolvedField, searchableFieldTypes);
      String finalFieldName =
          determineKeyword(
              resolvedField, resolvedFieldTypes, isTimeseries, opContext.getAspectRetriever());
      queryBuilder.should(QueryBuilders.termQuery(finalFieldName, Boolean.TRUE));
    }
    queryBuilder.minimumShouldMatch(1);
  }

  private static void evaluateNotPredicate(
      @Nonnull final Predicate predicate,
      @Nonnull final BoolQueryBuilder queryBuilder,
      boolean isTimeseries,
      Map<String, Set<SearchableAnnotation.FieldType>> searchableFieldTypes,
      Map<PathSpec, String> searchableFieldPaths,
      OperationContext opContext) {
    List<Operand> operands = predicate.getOperands().get();
    // Not always has one predicate type op
    Predicate subPredicate = (Predicate) operands.get(0).getExpression();
    BoolQueryBuilder subQueryBuilder = QueryBuilders.boolQuery();
    processPredicate(
        subPredicate,
        subQueryBuilder,
        isTimeseries,
        searchableFieldTypes,
        searchableFieldPaths,
        opContext);
    queryBuilder.mustNot(subQueryBuilder);
  }

  private static void evaluateQueryPredicate(@Nonnull final Predicate predicate) {
    throw new IllegalArgumentException(
        "Query not allowed as a top level predicate, should be applied to a particular "
            + "operation: "
            + predicate);
  }
}
