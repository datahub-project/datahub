package com.linkedin.datahub.graphql;

import graphql.ExecutionResult;
import graphql.GraphQLContext;
import graphql.analysis.QueryTraversalOptions;
import graphql.analysis.QueryTraverser;
import graphql.analysis.QueryVisitorFieldEnvironment;
import graphql.analysis.QueryVisitorStub;
import graphql.execution.AbortExecutionException;
import graphql.execution.ExecutionContext;
import graphql.execution.ValuesResolver;
import graphql.execution.instrumentation.InstrumentationContext;
import graphql.execution.instrumentation.InstrumentationState;
import graphql.execution.instrumentation.SimpleInstrumentationContext;
import graphql.execution.instrumentation.SimplePerformantInstrumentation;
import graphql.execution.instrumentation.parameters.InstrumentationExecuteOperationParameters;
import graphql.language.Field;
import graphql.language.FragmentSpread;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLFieldDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Aborts a query before it runs if the query is too deep or too complex.
 *
 * <p>Depth is the number of nested field levels. A top-level field has depth 1.
 *
 * <p>Complexity is the sum of these values:
 *
 * <ul>
 *   <li>1 for each field. {@code __typename} counts as 0.
 *   <li>The list size, if the {@code input} argument of a field is a list.
 *   <li>{@code 2 * input.count}, if the field is a lineage field and {@code input.count} is more
 *       than 1. A lineage field has {@code searchResults { entity { ...searchResultFields } } } in
 *       its selection set.
 * </ul>
 *
 * <p>A fragment counts one time in an operation, even if the query uses it many times. Fields that
 * {@code @skip} or {@code @include} remove do not count.
 *
 * <p>To calculate the depth and the fields, this class walks the query with {@link QueryTraverser}.
 * This is slow for large queries, so the class caches the result. Only the query, the operation and
 * the values of the Boolean variables can change the result, because {@code @skip} and
 * {@code @include} use only Boolean values. The results are cached in the {@link
 * GraphqlDocumentCache} entry of the query. {@link GraphqlPreparsedDocumentProvider} passes them to
 * this class through the {@code GraphQLContext} of the request.
 *
 * <p>The value of an {@code input} argument can be different for each request. So, they are
 * resolved separately per request.
 */
final class QueryCostInstrumentation extends SimplePerformantInstrumentation {

  private static final String INPUT_ARG = "input";
  private static final String COUNT_ARG = "count";
  private static final String SEARCH_RESULTS_FIELD = "searchResults";
  private static final String ENTITY_FIELD = "entity";
  private static final String SEARCH_RESULT_FIELDS_FRAGMENT = "searchResultFields";

  /**
   * GraphQLContext key of the cost results for the query: a {@code Map<CostKey, OperationCost>}.
   */
  static final String COSTS_CONTEXT_KEY = "datahub.queryCosts";

  /**
   * Maximum number of results to store for one query. This limit stops a query with many Boolean
   * variables from blowing up memory. Past the limit, each request walks the query again.
   */
  private static final int MAX_CACHED_COSTS = 16;

  private final int maxDepth;
  private final int maxComplexity;

  private record InputField(Field field, GraphQLFieldDefinition definition, boolean isLineage) {}

  /** An operation in the query, and the values of its Boolean variables. */
  record CostKey(@Nullable String operationName, List<Object> conditions) {}

  record OperationCost(int depth, int fieldCount, List<InputField> inputFields) {}

  QueryCostInstrumentation(int maxDepth, int maxComplexity) {
    this.maxDepth = maxDepth;
    this.maxComplexity = maxComplexity;
  }

  @Override
  public InstrumentationContext<ExecutionResult> beginExecuteOperation(
      InstrumentationExecuteOperationParameters parameters, InstrumentationState state) {
    ExecutionContext context = parameters.getExecutionContext();
    OperationCost cost = operationCost(context);
    if (cost.depth() > maxDepth) {
      throw new AbortExecutionException(
          "maximum query depth exceeded " + cost.depth() + " > " + maxDepth);
    }
    int complexity = complexity(cost, context);
    if (complexity > maxComplexity) {
      throw new AbortExecutionException(
          "maximum query complexity exceeded " + complexity + " > " + maxComplexity);
    }
    return SimpleInstrumentationContext.noOp();
  }

  private OperationCost operationCost(ExecutionContext context) {
    OperationDefinition operation = context.getOperationDefinition();
    Map<String, Object> variables = context.getCoercedVariables().toMap();
    // Only Boolean variables can change @skip and @include. Other variables get null in the key.
    List<Object> conditions = new ArrayList<>();
    for (VariableDefinition variable : operation.getVariableDefinitions()) {
      Object value = variables.get(variable.getName());
      conditions.add(value instanceof Boolean ? value : null);
    }
    // Null if the engine does not use GraphqlPreparsedDocumentProvider.
    Map<CostKey, OperationCost> costs = context.getGraphQLContext().get(COSTS_CONTEXT_KEY);
    if (costs == null) {
      return traverse(context);
    }
    CostKey key = new CostKey(operation.getName(), conditions);
    OperationCost cost = costs.get(key);
    if (cost == null) {
      cost = traverse(context);
      if (costs.size() < MAX_CACHED_COSTS) {
        costs.put(key, cost);
      }
    }
    return cost;
  }

  /**
   * Walks the operation. Finds its depth, its field count and its fields with an input argument.
   */
  private static OperationCost traverse(ExecutionContext context) {
    CostVisitor visitor = new CostVisitor();
    QueryTraverser.newQueryTraverser()
        .schema(context.getGraphQLSchema())
        .document(context.getDocument())
        .operationName(context.getExecutionInput().getOperationName())
        .coercedVariables(context.getCoercedVariables())
        // complexity() reads the argument values for each request.
        .options(QueryTraversalOptions.defaultOptions().coerceFieldArguments(false))
        .build()
        .visitPreOrder(visitor);
    return new OperationCost(visitor.depth, visitor.fieldCount, List.copyOf(visitor.inputFields));
  }

  private static final class CostVisitor extends QueryVisitorStub {
    private int depth;
    private int fieldCount;
    private final List<InputField> inputFields = new ArrayList<>();

    @Override
    public void visitField(QueryVisitorFieldEnvironment env) {
      int fieldDepth = 0;
      for (QueryVisitorFieldEnvironment e = env; e != null; e = e.getParentEnvironment()) {
        fieldDepth++;
      }
      depth = Math.max(depth, fieldDepth);
      if (env.isTypeNameIntrospectionField()) {
        return;
      }
      fieldCount++;
      if (env.getFieldDefinition().getArgument(INPUT_ARG) != null) {
        inputFields.add(
            new InputField(env.getField(), env.getFieldDefinition(), isLineage(env.getField())));
      }
    }
  }

  private static int complexity(OperationCost cost, ExecutionContext context) {
    long complexity = cost.fieldCount();
    for (InputField inputField : cost.inputFields()) {
      Map<String, Object> arguments =
          ValuesResolver.getArgumentValues(
              context.getGraphQLSchema().getCodeRegistry(),
              inputField.definition().getArguments(),
              inputField.field().getArguments(),
              context.getCoercedVariables(),
              GraphQLContext.getDefault(),
              Locale.getDefault());
      Object input = arguments.get(INPUT_ARG);
      if (input instanceof Map<?, ?> map) {
        if (inputField.isLineage() && map.get(COUNT_ARG) instanceof Integer count && count > 1) {
          complexity += 2L * count;
        }
      } else if (input instanceof List<?> list) {
        complexity += list.size();
      }
    }
    return (int) Math.min(complexity, Integer.MAX_VALUE);
  }

  private static boolean isLineage(Field field) {
    return children(field, Field.class)
        .filter(searchResults -> SEARCH_RESULTS_FIELD.equals(searchResults.getName()))
        .flatMap(searchResults -> children(searchResults, Field.class))
        .filter(entity -> ENTITY_FIELD.equals(entity.getName()))
        .flatMap(entity -> children(entity, FragmentSpread.class))
        .anyMatch(spread -> SEARCH_RESULT_FIELDS_FRAGMENT.equals(spread.getName()));
  }

  private static <T extends Selection<T>> Stream<T> children(Field field, Class<T> type) {
    SelectionSet selectionSet = field.getSelectionSet();
    return selectionSet == null ? Stream.empty() : selectionSet.getSelectionsOfType(type).stream();
  }
}
