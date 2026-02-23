package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.assertion.rule.AssertionAssignmentRuleInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to create Assertion Assignment Rule "automations", implemented by Metadata
 * Tests, which are continuously running processes used to keep assertion assignments up to date
 * based on entity filter criteria defined by the rule.
 */
@Slf4j
public class AssertionAssignmentRuleTestBuilder {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String RULE_TEST_CATEGORY = "AssertionAssignmentRules";

  // The set of entity types that should have the assertion assignment rule automation running.
  private static final Set<String> ENTITY_TYPES = AcrylConstants.ASSERTABLE_ENTITY_TYPES;

  public static TestInfo buildAssertionAssignmentRuleTest(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo) {
    final TestInfo testInfo = new TestInfo();
    testInfo.setName(String.format("Assertion Assignment Rule - %s", ruleUrn.toString()));
    testInfo.setCategory(RULE_TEST_CATEGORY);
    testInfo.setDescription(
        String.format(
            "This test was auto-generated to implement assertion assignment for rule with urn %s",
            ruleUrn));
    testInfo.setDefinition(buildTestDefinition(opContext, ruleUrn, ruleInfo));
    testInfo.setCreated(createSystemAuditStamp());
    testInfo.setLastUpdated(createSystemAuditStamp());
    testInfo.setSource(
        new TestSource()
            .setType(TestSourceType.ASSERTION_ASSIGNMENT_RULE)
            .setSourceEntity(ruleUrn));
    return testInfo;
  }

  private static TestDefinition buildTestDefinition(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo) {
    TestDefinition definition = new TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    try {
      definition.setJson(buildTestDefinitionJson(opContext, ruleUrn, ruleInfo));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to generate JSON test definition for assertion assignment rule!", e);
    }
    return definition;
  }

  private static String buildTestDefinitionJson(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn ruleUrn,
      @Nonnull final AssertionAssignmentRuleInfo ruleInfo)
      throws JsonProcessingException {

    ObjectNode definitionNode = OBJECT_MAPPER.createObjectNode();

    // Build Test Target Set: We target all eligible entity types.
    ObjectNode onNode = definitionNode.putObject("on");
    ArrayNode typesArray = onNode.putArray("types");
    ENTITY_TYPES.forEach(typesArray::add);

    // Build Test Rule Set: We verify that entities match the filter criteria defined by the rule.
    // Any entities which no longer match will fail the test.
    final @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs =
        EntitySpecUtils.getSearchableFieldsToPathSpecs(
            opContext.getEntityRegistry(), new ArrayList<>(ENTITY_TYPES));

    ObjectNode rulesNode = definitionNode.putObject("rules");
    ArrayNode orArray = rulesNode.putArray("or");
    orArray.addAll(
        MetadataTestServiceUtils.convertFilterToTestConditions(
            ruleInfo.getEntityFilter().getFilter(), searchableFieldsToPathSpecs));

    // Build Test Actions:
    //     1. We upsert managed assertions for any entities matching the current filter set.
    //     2. We remove managed assertions from any entities which no longer match.
    ObjectNode actionsNode = definitionNode.putObject("actions");
    ArrayNode passingArray = actionsNode.putArray("passing");
    ObjectNode upsertNode = passingArray.addObject();
    upsertNode.put("type", ActionType.UPSERT_ASSERTION_ASSIGNMENT_RULE.name());
    ObjectNode upsertParamsNode = upsertNode.putObject("params");
    upsertParamsNode.put(AssertionAssignmentRuleParams.RULE_URN, ruleUrn.toString());

    ArrayNode failingArray = actionsNode.putArray("failing");
    ObjectNode removeNode = failingArray.addObject();
    removeNode.put("type", ActionType.REMOVE_ASSERTION_ASSIGNMENT_RULE.name());
    ObjectNode removeParamsNode = removeNode.putObject("params");
    removeParamsNode.put(AssertionAssignmentRuleParams.RULE_URN, ruleUrn.toString());

    // Convert the def node to a formatted string
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(definitionNode);
  }

  private static AuditStamp createSystemAuditStamp() {
    return new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  private AssertionAssignmentRuleTestBuilder() {}
}
