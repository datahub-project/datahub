package com.linkedin.metadata.test.definition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.definition.operation.OperationParam;
import com.linkedin.metadata.test.definition.operation.ParamKeyConstants;
import com.linkedin.metadata.test.definition.operation.PredicateParam;
import com.linkedin.metadata.test.definition.operation.QueryParam;
import com.linkedin.metadata.test.definition.operation.StringParam;
import com.linkedin.metadata.test.eval.TestPredicateEvaluator;
import com.linkedin.metadata.test.exception.OperationParamsInvalidException;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;


/**
 * Utility class for deserializing JSON test definition
 */
@RequiredArgsConstructor
public class TestDefinitionProvider {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final TestPredicateEvaluator testPredicateEvaluator;

  public TestDefinition deserialize(Urn testUrn, String jsonTestDefinition) throws TestDefinitionParsingException {
    JsonNode parsedTestDefinition;
    try {
      parsedTestDefinition = OBJECT_MAPPER.readTree(jsonTestDefinition);
    } catch (JsonProcessingException e) {
      throw new TestDefinitionParsingException("Invalid JSON syntax", e);
    }
    if (!parsedTestDefinition.isObject() || !parsedTestDefinition.has("on") || !parsedTestDefinition.has("rules")) {
      throw new TestDefinitionParsingException(String.format(
          "Failed to deserialize test definition %s: test definition must have a on clause and a rules clause",
          jsonTestDefinition));
    }
    return new TestDefinition(testUrn, deserializeTargettingRule(parsedTestDefinition.get("on")),
        deserializeRule(parsedTestDefinition.get("rules")));
  }

  private TestTargetingRule deserializeTargettingRule(JsonNode jsonTargetingRule) {
    if (!jsonTargetingRule.isObject()) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize targeting rule %s: malformed targeting rule",
              jsonTargetingRule.toString()));
    }
    ObjectNode parsedTargetingRule = (ObjectNode) jsonTargetingRule;
    if (!parsedTargetingRule.has("types") || !parsedTargetingRule.get("types").isArray()) {
      throw new TestDefinitionParsingException(String.format(
          "Failed to deserialize targeting rule %s: targeting rule must contain field types with a list of types to target",
          jsonTargetingRule.toString()));
    }
    ArrayNode targetTypesJson = (ArrayNode) parsedTargetingRule.get("types");
    List<String> targetTypes =
        StreamSupport.stream(targetTypesJson.spliterator(), false).map(JsonNode::asText).collect(Collectors.toList());

    Optional<TestPredicate> targetingRules;
    if (parsedTargetingRule.has("match")) {
      targetingRules = Optional.of(deserializeRule(parsedTargetingRule.get("match")));
    } else {
      targetingRules = Optional.empty();
    }
    return new TestTargetingRule(targetTypes, targetingRules);
  }

  private TestPredicate deserializeRule(JsonNode jsonRule) {
    if (jsonRule.isArray()) {
      ArrayNode ruleArray = (ArrayNode) jsonRule;
      return deserializeCompositePredicate(ruleArray, "and", false);
    }
    if (jsonRule.isObject()) {
      ObjectNode ruleObject = (ObjectNode) jsonRule;
      boolean negated = false;
      if (ruleObject.has("negate")) {
        negated = ruleObject.get("negate").asBoolean();
      }
      if (ruleObject.has("or") && ruleObject.get("or").isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get("or");
        return deserializeCompositePredicate(ruleArray, "or", negated);
      }
      if (ruleObject.has("and") && ruleObject.get("and").isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get("and");
        return deserializeCompositePredicate(ruleArray, "and", negated);
      }
      return deserializeBasePredicate(ruleObject, negated);
    }
    throw new TestDefinitionParsingException(String.format("Failed to deserialize rule %s", jsonRule.toString()));
  }

  private TestPredicate deserializeCompositePredicate(ArrayNode childPredicates, String operation, boolean negated) {
    List<TestPredicate> deserializedChildPredicates = StreamSupport.stream(childPredicates.spliterator(), false)
        .map(this::deserializeRule)
        .collect(Collectors.toList());
    return new TestPredicate(operation,
        ImmutableMap.of(ParamKeyConstants.PREDICATES, new PredicateParam(deserializedChildPredicates)), negated);
  }

  private OperationParam createParam(JsonNode paramJson) {
    if (paramJson.isArray()) {
      ArrayNode paramArray = (ArrayNode) paramJson;
      return new StringParam(
          StreamSupport.stream(paramArray.spliterator(), false).map(JsonNode::asText).collect(Collectors.toList()));
    }
    return new StringParam(Collections.singletonList(paramJson.asText()));
  }

  private TestPredicate deserializeBasePredicate(ObjectNode testRule, boolean negated) {
    if (!testRule.has("query") || !testRule.get("query").isTextual()) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: query is a required field and must be a string",
              testRule.toString()));
    }
    String query = testRule.get("query").asText();
    if (!testRule.has("operation") || !testRule.get("operation").isTextual()) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: operation is a required field and must be a string",
              testRule.toString()));
    }
    String operation = testRule.get("operation").asText();
    if (!testPredicateEvaluator.isOperationValid(operation)) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: Unsupported operation %s", testRule.toString(), operation));
    }

    final Map<String, OperationParam> params = new HashMap<>();
    if (testRule.has("params")) {
      if (!testRule.get("params").isObject()) {
        throw new TestDefinitionParsingException(
            String.format("Failed to deserialize rule %s: params must be a map", testRule.toString()));
      }
      ObjectNode operationParams = (ObjectNode) testRule.get("params");
      operationParams.fields().forEachRemaining(entry -> params.put(entry.getKey(), createParam(entry.getValue())));
    }
    params.put(ParamKeyConstants.QUERY, new QueryParam(query));

    TestPredicate testPredicate = new TestPredicate(operation, params, negated);
    try {
      testPredicateEvaluator.validate(testPredicate);
    } catch (OperationParamsInvalidException e) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: failed to validate params for the operation",
              testRule.toString()), e);
    }
    return testPredicate;
  }
}
