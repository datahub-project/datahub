package com.linkedin.metadata.test.definition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.definition.expression.Expression;
import com.linkedin.metadata.test.definition.operator.Operand;
import com.linkedin.metadata.test.definition.operator.OperandConstants;
import com.linkedin.metadata.test.definition.operator.OperatorType;
import com.linkedin.metadata.test.definition.operator.Predicate;
import com.linkedin.metadata.test.definition.expression.Query;
import com.linkedin.metadata.test.definition.literal.StringListLiteral;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;

import static com.linkedin.metadata.Constants.*;


/**
 * Utility class for deserializing JSON-serialized Metadata Test definitions (the 'definition' block).
 *
 * Metadata Tests are encoded as equivalent to their YAML counterpart. In YAML, a Metadata Test definition
 * can be defined as:
 *
 *   definition:
 *     on:
 *       types:
 *         - {entity-type} # ex: dataset
 *       conditions:
 *          and:
 *            - property: {property name} # ex: ownership.owners.owner
 *              operator: {operator common name} # ex: equals
 *     rules:
 *       not:
 *         or:
 *           - property: {property name} # ex: editableDatasetProperties.description
 *             operator: {operator common name} # ex: contains
 *             value: "{the value to compare against}" # ex: "email address"
 *           - and:
 *             - property: {property name} # ex: editableDatasetProperties.description
 *               operator: {operator common name} # ex: regex_match
 *               value: "{the value to compare against}" # ex: ".*pii.*"
 *
 * Where the "on" block is used to define conditions for selecting entities in scope for the test.
 * And the "rules" block is used to define the conditions that entities must pass in order to pass the test.
 *
 * A "property" is simply a reference to a field that is resolvable via an URN. We have a single "resolver"
 * which uses the aspect graph to resolve the provided properties.
 *
 * An "operator" defines the type of comparison to do with the resolved property. For example, we can compare
 * it for equivalence, match against a regex, or something else. The full set of supported operators can be found
 * inside {@link OperatorType}.
 *
 * The  Metadata Test API technically allows for arbitrary nesting, but it's not recommended that you nest
 * further than one or two logical levels.
 *
 * By default, any array without a logical operator parent is treated as an implicit AND.
 *
 * # TODO: Define a set of useful property names.
 */
@RequiredArgsConstructor
public class TestDefinitionParser {

  private static final String LEGACY_OPERATION_FIELD = "operation"; // Use 'operator' instead.
  private static final String LEGACY_QUERY_FIELD = "query"; // Use 'property' instead.
  private static final String LEGACY_NEGATION_FIELD = "negate"; // Use 'not' instead.
  private static final String LEGACY_MATCH_FIELD = "match"; // Use 'conditions' instead.

  private static final String PROPERTY_FIELD = "property";
  private static final String OPERATOR_FIELD = "operator";
  private static final String PARAMS_FIELD = "params";

  // Logical Operators
  private static final String LOGICAL_AND_FIELD = "and";
  private static final String LOGICAL_OR_FIELD = "or";
  private static final String LOGICAL_NOT_FIELD = "not";

  // Top level test fields
  private static final String ON_FIELD = "on";
  private static final String ENTITY_TYPES_FIELD = "types";
  private static final String CONDITIONS_FIELD = "conditions";
  private static final String RULES_FIELD = "rules";
  private static final String ACTIONS_FIELD = "actions";

  // Actions
  private static final String PASSING_ACTIONS_FIELD = "passing";
  private static final String FAILING_ACTIONS_FIELD = "failing";
  private static final String ACTION_TYPE_FIELD = "type";
  private static final String ACTION_PARAMS_FIELD = "params";

  private static final Set<String> STANDARD_PREDICATE_FIELDS = ImmutableSet.of(
      LEGACY_OPERATION_FIELD,
      LEGACY_QUERY_FIELD,
      LEGACY_NEGATION_FIELD,
      PROPERTY_FIELD,
      OPERATOR_FIELD,
      PARAMS_FIELD
  );

  private static final Set<String> STANDARD_ACTION_FIELDS = ImmutableSet.of(
      ACTION_TYPE_FIELD,
      ACTION_PARAMS_FIELD
  );

  private static final TestActions EMPTY_ACTIONS = new TestActions(Collections.emptyList(), Collections.emptyList());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    int maxSize = Integer.parseInt(System.getenv().getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER.getFactory().setStreamReadConstraints(StreamReadConstraints.builder()
        .maxStringLength(maxSize).build());
  }

  private final PredicateEvaluator predicateEvaluator;

  public TestDefinition deserialize(Urn testUrn, String jsonTestDefinition) throws TestDefinitionParsingException {
    JsonNode parsedTestDefinition;
    try {
      parsedTestDefinition = OBJECT_MAPPER.readTree(jsonTestDefinition);
    } catch (JsonProcessingException e) {
      throw new TestDefinitionParsingException("Invalid JSON syntax", e);
    }
    if (!parsedTestDefinition.isObject() || !parsedTestDefinition.has(ON_FIELD) || !parsedTestDefinition.has(RULES_FIELD)) {
      throw new TestDefinitionParsingException(String.format(
          "Failed to deserialize test definitio for urn %s: %s: test definition must have a on clause and a rules clause",
          testUrn,
          jsonTestDefinition));
    }
    return new TestDefinition(
        testUrn,
        deserializeMatchConditions(parsedTestDefinition.get(ON_FIELD)),
        deserializeRule(parsedTestDefinition.get(RULES_FIELD)),
        deserializeActions(parsedTestDefinition.get(ACTIONS_FIELD)));
  }

  private TestMatch deserializeMatchConditions(JsonNode jsonTargetingRule) {
    if (!jsonTargetingRule.isObject()) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize targeting rule %s: malformed targeting rule",
              jsonTargetingRule.toString()));
    }
    ObjectNode parsedTargetingRule = (ObjectNode) jsonTargetingRule;
    if (!parsedTargetingRule.has(ENTITY_TYPES_FIELD) || !parsedTargetingRule.get(ENTITY_TYPES_FIELD).isArray()) {
      throw new TestDefinitionParsingException(String.format(
          "Failed to deserialize targeting rule %s: targeting rule must contain field types with a list of types to target",
          jsonTargetingRule.toString()));
    }
    ArrayNode targetTypesJson = (ArrayNode) parsedTargetingRule.get(ENTITY_TYPES_FIELD);
    List<String> targetTypes =
        StreamSupport.stream(targetTypesJson.spliterator(), false).map(JsonNode::asText).collect(Collectors.toList());

    Predicate targetingRules;
    if (parsedTargetingRule.has(CONDITIONS_FIELD) || parsedTargetingRule.has(LEGACY_MATCH_FIELD)) {
      targetingRules = deserializeRule(parsedTargetingRule.has(CONDITIONS_FIELD)
          ? parsedTargetingRule.get(CONDITIONS_FIELD)
          : parsedTargetingRule.get(LEGACY_MATCH_FIELD));
    } else {
      targetingRules = null;
    }
    return new TestMatch(targetTypes, targetingRules);
  }

  private Predicate deserializeRule(JsonNode jsonRule) {
    // TODO: Validate that single object doesn't contains multiple children (like AND and NOT). 
    if (jsonRule.isArray()) {
      ArrayNode ruleArray = (ArrayNode) jsonRule;
      return deserializeCompositePredicate(ruleArray, LOGICAL_AND_FIELD, false);
    }
    if (jsonRule.isObject()) {
      ObjectNode ruleObject = (ObjectNode) jsonRule;
      boolean negated = false;
      if (ruleObject.has(LOGICAL_NOT_FIELD)) {
        Predicate innerPredicate = deserializeRule(ruleObject.get(LOGICAL_NOT_FIELD));
        return new Predicate(
            OperatorType.NOT,
            createNotOperands(innerPredicate)
        );
      }
      // Deprecated.
      if (ruleObject.has(LEGACY_NEGATION_FIELD)) {
        negated = ruleObject.get(LEGACY_NEGATION_FIELD).asBoolean();
      }
      if (ruleObject.has(LOGICAL_OR_FIELD) && ruleObject.get(LOGICAL_OR_FIELD).isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get(LOGICAL_OR_FIELD);
        return deserializeCompositePredicate(ruleArray, LOGICAL_OR_FIELD, negated);
      }
      if (ruleObject.has(LOGICAL_AND_FIELD) && ruleObject.get(LOGICAL_AND_FIELD).isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get(LOGICAL_AND_FIELD);
        return deserializeCompositePredicate(ruleArray, LOGICAL_AND_FIELD, negated);
      }
      return deserializeBasePredicate(ruleObject, negated);
    }
    throw new TestDefinitionParsingException(String.format("Failed to deserialize rule %s", jsonRule.toString()));
  }

  private TestActions deserializeActions(@Nullable JsonNode jsonActions) {
    if (jsonActions == null) {
      return EMPTY_ACTIONS;
    }
    if (!jsonActions.isObject()) {
      throw new TestDefinitionParsingException(String.format("Failed to deserialize actions %s. Actions block must be an object! Found %s",
          jsonActions.toString(), jsonActions.getNodeType()));
    }
    ObjectNode ruleObject = (ObjectNode) jsonActions;

    final List<TestAction> failingActions = new ArrayList<>();
    final List<TestAction> passingActions = new ArrayList<>();

    if (ruleObject.has(PASSING_ACTIONS_FIELD)) {
      passingActions.addAll(deserializeActionsList(ruleObject.get(PASSING_ACTIONS_FIELD)));
    }
    if (ruleObject.has(FAILING_ACTIONS_FIELD)) {
      failingActions.addAll(deserializeActionsList(ruleObject.get(FAILING_ACTIONS_FIELD)));
    }
    return new TestActions(passingActions, failingActions);
  }

  private List<TestAction> deserializeActionsList(JsonNode jsonActionsList) {
    if (!jsonActionsList.isArray()) {
      throw new TestDefinitionParsingException(String.format("Failed to deserialize actions list %s. Actions list must be a list! Found %s",
          jsonActionsList.toString(), jsonActionsList.getNodeType()));
    }
    ArrayNode actionsList = (ArrayNode) jsonActionsList;
    final List<TestAction> actions = new ArrayList<>();
    for (JsonNode node : actionsList) {
      actions.add(deserializeAction(node));
    }
    return actions;
  }


  private TestAction deserializeAction(JsonNode jsonAction) {
    if (!jsonAction.isObject()) {
      throw new TestDefinitionParsingException(String.format("Failed to deserialize action %s. Actions definition must be an object! Found %s",
          jsonAction.toString(), jsonAction.getNodeType()));
    }
    ObjectNode action = (ObjectNode) jsonAction;

    ActionType actionType = deserializeTestActionType(action);
    Map<String, List<String>> actionParams = deserializeTestActionParams(action);
    return new TestAction(actionType, actionParams);
  }

  private ActionType deserializeTestActionType(ObjectNode action) {
    if (action.has(ACTION_TYPE_FIELD) && action.get(ACTION_TYPE_FIELD).isTextual()) {
      String actionType = action.get(ACTION_TYPE_FIELD).asText();
      try {
        return ActionType.fromCommonName(actionType);
      } catch (IllegalArgumentException e) {
        throw new TestDefinitionParsingException(String.format("Failed to deserialize action %s. Action has an unrecognized 'type' %s",
            action.toString(),
            actionType));
      }
    }
    throw new TestDefinitionParsingException(String.format("Failed to deserialize actions %s. Action must have a valid 'type' field of type string.",
        action.toString()));
  }

  private Map<String, List<String>> deserializeTestActionParams(ObjectNode action) {
    Map<String, List<String>> result = new HashMap<>();
    if (action.has(ACTION_PARAMS_FIELD)) {
      JsonNode actionParamsNode = action.get(ACTION_PARAMS_FIELD);
      if (!actionParamsNode.isObject()) {
        throw new TestDefinitionParsingException(String.format("Failed to deserialize action %s."
                + " Action must have a valid params of type map or object. Found %s",
            action.toString(),
            actionParamsNode.getNodeType()));
      }
      ObjectNode paramsObject = (ObjectNode) action.get(ACTION_PARAMS_FIELD);
      for (Iterator<String> it = paramsObject.fieldNames(); it.hasNext(); ) {
        String fieldName = it.next();
        result.put(fieldName, deserializeTestActionParam(paramsObject.get(fieldName)));
      }
    } else {
      // Treat the remaining fields as implicit parameters.
      for (Iterator<Map.Entry<String, JsonNode>> it = action.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> next = it.next();
        if (!STANDARD_ACTION_FIELDS.contains(next.getKey())) {
          result.put(next.getKey(), deserializeTestActionParam(next.getValue()));
        }
      }
    }
    return result;
  }

  private List<String> deserializeTestActionParam(JsonNode param) {
    if (param.isTextual()) {
      // Simply case into an array
      return Collections.singletonList(param.asText());
    } else if (param.isArray()) {
      ArrayNode paramArray = (ArrayNode) param;
      List<String> result = new ArrayList<>();
      for (JsonNode paramNode : paramArray) {
        if (paramNode.isTextual()) {
          result.add(paramNode.asText());
        } else {
          throw new TestDefinitionParsingException(String.format("Failed to deserialize action param %s. Action param must be of type string or list of string",
              param.toString()));
        }
      }
      return result;
    }
    throw new TestDefinitionParsingException(String.format("Failed to deserialize action param %s. Action param must be of type string or list of string",
        param.toString()));
  }

  private Predicate deserializeCompositePredicate(ArrayNode childPredicates, String operation, boolean negated) {
    List<Expression> deserializedChildPredicates = StreamSupport.stream(childPredicates.spliterator(), false)
        .map(this::deserializeRule)
        .map(pred -> (Expression) pred)
        .collect(Collectors.toList());
    return Predicate.of(OperatorType.fromCommonName(operation), deserializedChildPredicates, negated);
  }

  private Expression createParam(JsonNode paramJson) {
    if (paramJson.isArray()) {
      ArrayNode paramArray = (ArrayNode) paramJson;
      return new StringListLiteral(
          StreamSupport.stream(paramArray.spliterator(), false).map(JsonNode::asText).collect(Collectors.toList()));
    }
    return new StringListLiteral(Collections.singletonList(paramJson.asText()));
  }

  private Predicate deserializeBasePredicate(ObjectNode testRule, boolean negated) {
    if (!hasTextField(testRule, PROPERTY_FIELD) && !hasTextField(testRule, LEGACY_QUERY_FIELD)) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: property or query is a required field and must be a string",
              testRule.toString()));
    }
    String query = testRule.has(PROPERTY_FIELD) ? testRule.get(PROPERTY_FIELD).asText() : testRule.get(LEGACY_QUERY_FIELD).asText();
    if (!hasTextField(testRule, OPERATOR_FIELD) && !hasTextField(testRule, LEGACY_OPERATION_FIELD)) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: operation is a required field and must be a string",
              testRule.toString()));
    }
    String operation = testRule.has(OPERATOR_FIELD) ? testRule.get(OPERATOR_FIELD).asText() : testRule.get(LEGACY_OPERATION_FIELD).asText();
    if (!predicateEvaluator.isOperationValid(operation)) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: Unsupported operation %s", testRule.toString(), operation));
    }

    final List<Operand> operands = new ArrayList<>();
    operands.add(new Operand(0, OperandConstants.QUERY, new Query(query)));
    addLeftHandOperands(operands, testRule);

    Predicate predicate = new Predicate(OperatorType.fromCommonName(operation), operands, negated);
    try {
      predicateEvaluator.validate(predicate);
    } catch (InvalidOperandException e) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: failed to validate params for the operation",
              testRule.toString()), e);
    }
    return predicate;
  }

  /**
   * In this method we deserialize and add operands serving as inputs to a predicate operator.
   *
   * Operands can either be placed under a top-level 'params' field (legacy),
   * or simply appended to the predicate as additional key value pairs.
   *
   * In option 1, the predicate appears as follows:
   *
   * - property: datasetProperties.description
   *   operator: contains
   *   params:
   *     - value: some_substring
   *
   * In option 2, the predicate appears as follows:
   *
   * - property: datasetProperties.description
   *   operator: contains
   *   value: some_substring
   *
   * Which simply removes the need for an additional explicit level of nesting.
   */
  private void addLeftHandOperands(List<Operand> base, ObjectNode predicate) {
    if (predicate.has(PARAMS_FIELD)) {
      addLegacyParamValues(base, predicate);
    } else {
      addValues(base, predicate);
    }
  }

  private void addLegacyParamValues(List<Operand> base, ObjectNode predicateJson) {
      // If params field is set, use the explicit params map.
      if (!predicateJson.get(PARAMS_FIELD).isObject()) {
        throw new TestDefinitionParsingException(
            String.format("Failed to deserialize rule %s: params must be a map", predicateJson.toString()));
      }
      ObjectNode explicitOperands = (ObjectNode) predicateJson.get(PARAMS_FIELD);
    int currIndex = 1;
    for (Iterator<Map.Entry<String, JsonNode>> it = explicitOperands.fields(); it.hasNext(); ) {
        Map.Entry<String, JsonNode> next = it.next();
        base.add(new Operand(currIndex, next.getKey(), createParam(next.getValue())));
        currIndex++;
      }
  }

  private void addValues(List<Operand> base, ObjectNode predicateJson) {
    // Use all non-standard fields as named params.
    int currIndex = 1;
    for (Iterator<Map.Entry<String, JsonNode>> it = predicateJson.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> next = it.next();
      if (!STANDARD_PREDICATE_FIELDS.contains(next.getKey())) {
        // Found unrecognized field - make it a named-operand.
        base.add(new Operand(currIndex, next.getKey(), createParam(next.getValue())));
        currIndex++;
      }
    }
  }

  private static List<Operand> createNotOperands(Predicate base) {
    final List<Operand> result = new ArrayList<>();
    result.add(new Operand(0, base));
    return result;
  }

  private static boolean hasTextField(ObjectNode node, String field) {
    return node.has(field) && node.get(field).isTextual();
  }
}
