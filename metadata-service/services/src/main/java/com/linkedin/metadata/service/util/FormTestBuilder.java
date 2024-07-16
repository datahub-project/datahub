package com.linkedin.metadata.service.util;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormPrompt;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to create Form "automations", implemented by Metadata Tests, which are
 * continuously running processes used to keep form assignments up to date, along with verifying
 * that form requirements (prompts) are continuing to be met through time.
 */
@Slf4j
public class FormTestBuilder {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String FORM_TEST_CATEGORY = "Forms";

  // The set of entity types that should have the form assignment automation running.
  private static final Set<String> FORM_ASSIGNMENT_ENTITY_TYPES =
      ImmutableSet.of(
          DATASET_ENTITY_NAME,
          CONTAINER_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          CHART_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME);

  public static TestInfo buildFormAssignmentTest(
      @Nonnull final Urn formUrn, @Nonnull final DynamicFormAssignment assignment) {
    final TestInfo testInfo = new TestInfo();
    testInfo.setName(String.format("Form Assignment Test - %s", formUrn.toString()));
    testInfo.setCategory(FORM_TEST_CATEGORY);
    testInfo.setDescription(
        String.format(
            "This test was auto-generated to implement form assignment for form with urn %s",
            formUrn));
    testInfo.setDefinition(buildFormAssignmentTestDefinition(formUrn, assignment));
    testInfo.setCreated(createSystemAuditStamp());
    testInfo.setLastUpdated(createSystemAuditStamp());
    testInfo.setSource(new TestSource().setType(TestSourceType.FORMS).setSourceEntity(formUrn));
    return testInfo;
  }

  public static TestInfo buildFormPromptCompletionTest(
      @Nonnull final Urn formUrn, @Nonnull final FormPrompt prompt) {
    final TestInfo testInfo = new TestInfo();
    testInfo.setName(
        String.format(
            "Form Prompts Test - %s, Prompt Id - %s", formUrn.toString(), prompt.getId()));
    testInfo.setCategory(FORM_TEST_CATEGORY);
    testInfo.setDescription(
        String.format(
            "This test was auto-generated to implement form assignment for form with urn %s",
            formUrn));
    testInfo.setDefinition(buildFormPromptCompletionTestDefinition(formUrn, prompt));
    testInfo.setCreated(createSystemAuditStamp());
    testInfo.setLastUpdated(createSystemAuditStamp());
    testInfo.setSource(new TestSource().setType(TestSourceType.FORMS).setSourceEntity(formUrn));
    return testInfo;
  }

  public static TestDefinition buildFormAssignmentTestDefinition(
      @Nonnull final Urn formUrn, @Nonnull final DynamicFormAssignment assignment) {
    TestDefinition definition = new TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    try {
      // Convert completion test to JSON.
      definition.setJson(buildFormAssignmentTestDefinitionJson(formUrn, assignment));
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate JSON test definition for form assignment!", e);
    }
    log.info(definition.toString());
    return definition;
  }

  public static TestDefinition buildFormPromptCompletionTestDefinition(
      @Nonnull final Urn formUrn, @Nonnull final FormPrompt prompt) {
    TestDefinition definition = new TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    try {
      // Convert completion test to JSON.
      definition.setJson(buildFormPromptCompletionTestDefinitionJson(formUrn, prompt));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to generate JSON test definition for form prompt completion!", e);
    }
    log.info(definition.toString());
    return definition;
  }

  private static String buildFormAssignmentTestDefinitionJson(
      @Nonnull final Urn formUrn, @Nonnull final DynamicFormAssignment assignment)
      throws JsonProcessingException {

    ObjectNode definitionNode = OBJECT_MAPPER.createObjectNode();

    // Build Test Target Set: We target all eligible entity types which
    //     1. Already have the form assigned (in case we must remove it)
    //     2. Need to have the form assigned based on the current dynamic filters (in case we need
    // to add it)

    ObjectNode onNode = definitionNode.putObject("on");
    ArrayNode typesArray = onNode.putArray("types");
    FORM_ASSIGNMENT_ENTITY_TYPES.forEach(typesArray::add);

    // And those which have the current form applied them.
    ObjectNode conditionsNode = onNode.putObject("conditions");
    ArrayNode orConditions = conditionsNode.putArray("or");

    // Case 1: Target Entities with which ALREADY have the form applied to it.
    ObjectNode hasFormAssignedConditions = orConditions.addObject();

    ObjectNode incompleteFormsAssignmentNode = OBJECT_MAPPER.createObjectNode();
    ObjectNode completeFormsAssignmentNode = OBJECT_MAPPER.createObjectNode();

    incompleteFormsAssignmentNode.put("property", "forms.incompleteForms.urn");
    incompleteFormsAssignmentNode.put("operator", "equals");
    incompleteFormsAssignmentNode.put(
        "values", OBJECT_MAPPER.createArrayNode().add(formUrn.toString()));

    completeFormsAssignmentNode.put("property", "forms.completedForms.urn");
    completeFormsAssignmentNode.put("operator", "equals");
    completeFormsAssignmentNode.put(
        "values", OBJECT_MAPPER.createArrayNode().add(formUrn.toString()));

    hasFormAssignedConditions.set(
        "or",
        OBJECT_MAPPER
            .createArrayNode()
            .add(incompleteFormsAssignmentNode)
            .add(completeFormsAssignmentNode));

    // Case 2: Target Entities which do not have the form applied, but need it.
    ObjectNode entityFiltersNode = orConditions.addObject();
    ArrayNode orArray = entityFiltersNode.putArray("or");
    orArray.addAll(buildFormAssignmentTestConditions(assignment.getFilter()));

    // Build Test Rule Set: We verify that entities match the dynamic filter set that is currently
    // required for the form.
    // Any entities which have the form assigned, but do not match the current target set, will fail
    // the test.
    ObjectNode rulesNode = definitionNode.putObject("rules");
    orArray = rulesNode.putArray("or");
    orArray.addAll(buildFormAssignmentTestConditions(assignment.getFilter()));

    // Build Test Actions: Two important things happen here.
    //     1. We assign the form for any entities matching the current filter set.
    //     2. We remove the form from any entities which no longer match the current filter set.
    ObjectNode actionsNode = definitionNode.putObject("actions");
    ArrayNode passingArray = actionsNode.putArray("passing");
    ObjectNode assignFormNode = passingArray.addObject();
    assignFormNode.put("type", "ASSIGN_FORM");
    ObjectNode assignFormParamsNode = assignFormNode.putObject("params");
    assignFormParamsNode.put("formUrn", formUrn.toString());

    ArrayNode failingArray = actionsNode.putArray("failing");
    ObjectNode unassignFormNode = failingArray.addObject();
    unassignFormNode.put("type", "UNASSIGN_FORM");
    ObjectNode unassignFormParamsNode = unassignFormNode.putObject("params");
    unassignFormParamsNode.put("formUrn", formUrn.toString());

    // Convert the def node to a formatted string
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(definitionNode);
  }

  public static String buildFormPromptCompletionTestDefinitionJson(
      @Nonnull final Urn formUrn, @Nonnull final FormPrompt prompt) throws JsonProcessingException {

    ObjectNode definitionNode = OBJECT_MAPPER.createObjectNode();

    // Build Test Target Set: We target all eligible entity types which have the form assigned to it
    // already.
    ObjectNode onNode = definitionNode.putObject("on");
    ArrayNode typesArray = onNode.putArray("types");
    FORM_ASSIGNMENT_ENTITY_TYPES.forEach(typesArray::add);

    ObjectNode conditionsNode = onNode.putObject("conditions");

    ObjectNode incompleteFormsAssignmentNode = OBJECT_MAPPER.createObjectNode();
    ObjectNode completeFormsAssignmentNode = OBJECT_MAPPER.createObjectNode();

    incompleteFormsAssignmentNode.put("property", "forms.incompleteForms.urn");
    incompleteFormsAssignmentNode.put("operator", "equals");
    incompleteFormsAssignmentNode.put(
        "values", OBJECT_MAPPER.createArrayNode().add(formUrn.toString()));

    completeFormsAssignmentNode.put("property", "forms.completedForms.urn");
    completeFormsAssignmentNode.put("operator", "equals");
    completeFormsAssignmentNode.put(
        "values", OBJECT_MAPPER.createArrayNode().add(formUrn.toString()));

    conditionsNode.set(
        "or",
        OBJECT_MAPPER
            .createArrayNode()
            .add(incompleteFormsAssignmentNode)
            .add(completeFormsAssignmentNode));

    // Build Rules Set: We verify that any entities with the form applied must be matching the
    // criteria outlined
    // by a specific prompt (question) within the form. If the prompt condition is NOT met, then the
    // entity will be considered
    // failing. Conversely, if the prompt requirement is met, then the entity will be considered
    // passing.
    ObjectNode rulesNode = definitionNode.putObject("rules");

    switch (prompt.getType()) {
      case STRUCTURED_PROPERTY:
        ArrayNode structuredPropsAndArray = rulesNode.putArray("and");
        structuredPropsAndArray.addAll(buildStructuredPropertiesTestConditions(prompt));
        break;
      case OWNERSHIP:
        ArrayNode ownershipAndArray = rulesNode.putArray("and");
        ownershipAndArray.addAll(buildOwnershipTestConditions(prompt));
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported prompt type %s provided. Form urn: %s, Prompt Id: %s.",
                prompt.getType(), formUrn.toString(), prompt.getId()));
    }

    // Build Test Actions: For entities which FAIL the test, we mark the prompt as "incomplete" for
    // the entity, so that
    // users see the action to complete the prompt again.
    // For entities which PASS the test, we do nothing. This is where in the future we can plug in
    // logic to auto-mark the
    // prompt as completed without requiring any explicit user action.
    ObjectNode actionsNode = definitionNode.putObject("actions");
    actionsNode.putArray("passing"); // Do nothing on pass.
    ArrayNode failingArray = actionsNode.putArray("failing");
    ObjectNode addActionNode = failingArray.addObject();
    addActionNode.put("type", "SET_FORM_PROMPT_INCOMPLETE");
    ObjectNode actionParamsNode = addActionNode.putObject("params");
    actionParamsNode.put("formUrn", formUrn.toString());
    actionParamsNode.put("formPromptId", prompt.getId());

    // Convert the rootNode to a formatted string
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(definitionNode);
  }

  private static List<JsonNode> buildStructuredPropertiesTestConditions(
      @Nonnull final FormPrompt prompt) {
    ObjectNode propertyNode = OBJECT_MAPPER.createObjectNode();
    propertyNode.put(
        "property",
        "structuredProperties." + prompt.getStructuredPropertyParams().getUrn().toString());
    propertyNode.put("operator", "exists");
    return ImmutableList.of(propertyNode);
  }

  private static List<JsonNode> buildOwnershipTestConditions(@Nonnull final FormPrompt prompt) {
    ObjectNode propertyNode = OBJECT_MAPPER.createObjectNode();
    propertyNode.put("property", "ownership.owners.owner");
    propertyNode.put("operator", "exists");
    return ImmutableList.of(propertyNode);
  }

  private static List<JsonNode> buildFormAssignmentTestConditions(@Nonnull final Filter filter) {
    // We know we have an OR of ANDs to deal with.
    if (filter.hasOr()) {
      final List<JsonNode> orConditions = new ArrayList<>();
      for (ConjunctiveCriterion orCondition : filter.getOr()) {
        ObjectNode andNode = OBJECT_MAPPER.createObjectNode();
        final List<JsonNode> andConditions = new ArrayList<>();
        // Build AND conditions:
        for (Criterion andCriterion : orCondition.getAnd()) {
          andConditions.add(buildFormAssignmentTestCriterion(andCriterion));
        }
        andNode.put("and", OBJECT_MAPPER.createArrayNode().addAll(andConditions));
        orConditions.add(andNode);
      }
      return orConditions;
    }
    log.warn(String.format("Found form assignment with empty filter conditions! %s", filter));
    return Collections.emptyList();
  }

  private static ObjectNode buildFormAssignmentTestCriterion(@Nonnull final Criterion criterion) {

    final String finalField = mapFilterField(criterion.getField());
    final String finalOperator = mapFilterOperator(criterion.getCondition());
    final List<String> finalValues = criterion.getValues();

    final ObjectNode predicateNode = OBJECT_MAPPER.createObjectNode();
    predicateNode.put("property", finalField);
    predicateNode.put("operator", finalOperator);
    ArrayNode valuesNode = OBJECT_MAPPER.createArrayNode();
    finalValues.forEach(valuesNode::add);
    predicateNode.put("values", valuesNode);

    // Finally, if we are negating the conditions, negate that here.
    if (criterion.isNegated()) {
      ObjectNode notNode = OBJECT_MAPPER.createObjectNode();
      notNode.put("not", predicateNode);
      return notNode;
    }

    // Otherwise, return the original predicate node.
    return predicateNode;
  }

  @Nonnull
  private static String mapFilterField(@Nonnull final String filterField) {
    // TODO: Refactor this for extensibility.
    switch (filterField) {
      case "platform":
      case "platform.keyword":
        return "dataPlatformInstance.platform";
      case "domain":
      case "domains":
      case "domains.keyword":
        return "domains.domains";
      case "container":
      case "container.keyword":
        return "container.container";
      case "_entityType":
      case "entityType":
        return "entityType";
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported filter field %s provided in test conditions", filterField));
    }
  }

  @Nonnull
  private static String mapFilterOperator(@Nonnull final Condition filterCondition) {
    // TODO: Refactor this for extensibility.
    switch (filterCondition) {
      case EQUAL:
      case IN:
        return "equals";
      case GREATER_THAN:
        return "greater_than";
      case LESS_THAN:
        return "less_than";
      case EXISTS:
        return "exists";
      case CONTAIN:
        return "contains_str";
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported filter operator %s provided in test conditions", filterCondition));
    }
  }

  public static Urn createTestUrnForFormPrompt(
      @Nonnull final Urn formUrn, @Nonnull final FormPrompt formPrompt) {
    final String testId =
        String.format(
            "%s-%s",
            URLEncoder.encode(formUrn.toString(), StandardCharsets.UTF_8), formPrompt.getId());
    return UrnUtils.getUrn(String.format("urn:li:%s:%s", TEST_ENTITY_NAME, testId));
  }

  public static Urn createTestUrnForFormAssignment(@Nonnull final Urn formUrn) {
    final String testId =
        String.format("%s", URLEncoder.encode(formUrn.toString(), StandardCharsets.UTF_8));
    return UrnUtils.getUrn(String.format("urn:li:%s:%s", TEST_ENTITY_NAME, testId));
  }

  private static AuditStamp createSystemAuditStamp() {
    return new AuditStamp()
        .setTime(System.currentTimeMillis())
        .setActor(UrnUtils.getUrn(SYSTEM_ACTOR));
  }

  private FormTestBuilder() {}
}
