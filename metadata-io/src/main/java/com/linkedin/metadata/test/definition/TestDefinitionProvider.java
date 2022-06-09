package com.linkedin.metadata.test.definition;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.eval.LeafTestPredicateEvaluator;
import com.linkedin.metadata.test.exception.TestDefinitionParsingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;

import static com.linkedin.metadata.test.definition.CompositeTestPredicate.CompositionOperation.AND;
import static com.linkedin.metadata.test.definition.CompositeTestPredicate.CompositionOperation.OR;


/**
 * Utility class for deserializing JSON test definition
 */
@RequiredArgsConstructor
public class TestDefinitionProvider {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final LeafTestPredicateEvaluator leafTestPredicateEvaluator;

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
      return deserializeCompositeRules(ruleArray, AND, false);
    }
    if (jsonRule.isObject()) {
      ObjectNode ruleObject = (ObjectNode) jsonRule;
      boolean negated = false;
      if (ruleObject.has("negate")) {
        negated = ruleObject.get("negate").asBoolean();
      }
      if (ruleObject.has("or") && ruleObject.get("or").isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get("or");
        return deserializeCompositeRules(ruleArray, OR, negated);
      }
      if (ruleObject.has("and") && ruleObject.get("and").isArray()) {
        ArrayNode ruleArray = (ArrayNode) jsonRule.get("and");
        return deserializeCompositeRules(ruleArray, AND, negated);
      }
      return deserializeUnitTestRule(ruleObject, negated);
    }
    throw new TestDefinitionParsingException(String.format("Failed to deserialize rule %s", jsonRule.toString()));
  }

  private TestPredicate deserializeCompositeRules(ArrayNode rules, CompositeTestPredicate.CompositionOperation operation,
      boolean negated) {
    return new CompositeTestPredicate(operation,
        StreamSupport.stream(rules.spliterator(), false).map(this::deserializeRule).collect(Collectors.toList()),
        negated);
  }

  private LeafTestPredicate deserializeUnitTestRule(ObjectNode testRule, boolean negated) {
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
    if (!leafTestPredicateEvaluator.isOperationValid(operation)) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: Unsupported operation %s", testRule.toString(), operation));
    }

    Map<String, Object> params;
    if (testRule.has("params")) {
      if (!testRule.get("params").isObject()) {
        throw new TestDefinitionParsingException(
            String.format("Failed to deserialize rule %s: params must be a map", testRule.toString()));
      }
      params = OBJECT_MAPPER.convertValue(testRule.get("params"), new TypeReference<Map<String, Object>>() {
      });
    } else {
      params = Collections.emptyMap();
    }

    LeafTestPredicate unitTestRule = new LeafTestPredicate(query, operation, params, negated);
    try {
      leafTestPredicateEvaluator.validate(unitTestRule);
    } catch (TestDefinitionParsingException e) {
      throw new TestDefinitionParsingException(
          String.format("Failed to deserialize rule %s: failed to validate params for the operation",
              testRule.toString()), e);
    }
    return unitTestRule;
  }
}
