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
import com.linkedin.data.schema.PathSpec;
import com.linkedin.form.DomainParams;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.GlossaryTermsParams;
import com.linkedin.form.OwnershipParams;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestSource;
import com.linkedin.test.TestSourceType;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
          DASHBOARD_ENTITY_NAME,
          ML_MODEL_ENTITY_NAME,
          ML_MODEL_GROUP_ENTITY_NAME,
          ML_FEATURE_TABLE_ENTITY_NAME,
          ML_FEATURE_ENTITY_NAME,
          ML_PRIMARY_KEY_ENTITY_NAME,
          GLOSSARY_TERM_ENTITY_NAME,
          GLOSSARY_NODE_ENTITY_NAME,
          DOMAIN_ENTITY_NAME,
          DATA_PRODUCT_ENTITY_NAME);

  public static TestInfo buildFormAssignmentTest(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment assignment) {
    final TestInfo testInfo = new TestInfo();
    testInfo.setName(String.format("Form Assignment Test - %s", formUrn.toString()));
    testInfo.setCategory(FORM_TEST_CATEGORY);
    testInfo.setDescription(
        String.format(
            "This test was auto-generated to implement form assignment for form with urn %s",
            formUrn));
    testInfo.setDefinition(buildFormAssignmentTestDefinition(opContext, formUrn, assignment));
    testInfo.setCreated(createSystemAuditStamp());
    testInfo.setLastUpdated(createSystemAuditStamp());
    testInfo.setSource(new TestSource().setType(TestSourceType.FORMS).setSourceEntity(formUrn));
    return testInfo;
  }

  public static TestInfo buildFormPromptCompletionTest(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final FormPrompt prompt) {
    final TestInfo testInfo = new TestInfo();
    testInfo.setName(
        String.format(
            "Form Prompts Test - %s, Prompt Id - %s", formUrn.toString(), prompt.getId()));
    testInfo.setCategory(FORM_TEST_CATEGORY);
    testInfo.setDescription(
        String.format(
            "This test was auto-generated to implement form assignment for form with urn %s",
            formUrn));
    testInfo.setDefinition(buildFormPromptCompletionTestDefinition(opContext, formUrn, prompt));
    testInfo.setCreated(createSystemAuditStamp());
    testInfo.setLastUpdated(createSystemAuditStamp());
    testInfo.setSource(
        new TestSource().setType(TestSourceType.FORM_PROMPT).setSourceEntity(formUrn));
    return testInfo;
  }

  public static TestDefinition buildFormAssignmentTestDefinition(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment assignment) {
    TestDefinition definition = new TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    try {
      // Convert completion test to JSON.
      definition.setJson(buildFormAssignmentTestDefinitionJson(opContext, formUrn, assignment));
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate JSON test definition for form assignment!", e);
    }
    log.info(definition.toString());
    return definition;
  }

  public static TestDefinition buildFormPromptCompletionTestDefinition(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final FormPrompt prompt) {
    TestDefinition definition = new TestDefinition();
    definition.setType(TestDefinitionType.JSON);
    try {
      // Convert completion test to JSON.
      definition.setJson(buildFormPromptCompletionTestDefinitionJson(opContext, formUrn, prompt));
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to generate JSON test definition for form prompt completion!", e);
    }
    log.info(definition.toString());
    return definition;
  }

  private static String buildFormAssignmentTestDefinitionJson(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final DynamicFormAssignment assignment)
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
    // First, get searchable fields to aspect paths map so we can convert a filter to metadata test
    // aspect path specs
    final @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs =
        EntitySpecUtils.getSearchableFieldsToPathSpecs(
            opContext.getEntityRegistry(), new ArrayList<>(FORM_ASSIGNMENT_ENTITY_TYPES));

    ObjectNode entityFiltersNode = orConditions.addObject();
    ArrayNode orArray = entityFiltersNode.putArray("or");
    orArray.addAll(
        MetadataTestServiceUtils.convertFilterToTestConditions(
            assignment.getFilter(), searchableFieldsToPathSpecs));

    // Build Test Rule Set: We verify that entities match the dynamic filter set that is currently
    // required for the form.
    // Any entities which have the form assigned, but do not match the current target set, will fail
    // the test.
    ObjectNode rulesNode = definitionNode.putObject("rules");
    orArray = rulesNode.putArray("or");
    orArray.addAll(
        MetadataTestServiceUtils.convertFilterToTestConditions(
            assignment.getFilter(), searchableFieldsToPathSpecs));

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
      @Nonnull OperationContext opContext,
      @Nonnull final Urn formUrn,
      @Nonnull final FormPrompt prompt)
      throws JsonProcessingException {

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
      case DOCUMENTATION:
        ArrayNode documentationOrArray = rulesNode.putArray("or");
        documentationOrArray.addAll(buildDocumentationTestConditions(opContext));
        break;
      case GLOSSARY_TERMS:
        ArrayNode glossaryTermsAndArray = rulesNode.putArray("or");
        glossaryTermsAndArray.addAll(buildGlossaryTermsTestConditions(prompt));
        break;
      case DOMAIN:
        ArrayNode domainAndArray = rulesNode.putArray("or");
        domainAndArray.addAll(buildDomainTestConditions(prompt));
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
    OwnershipParams promptParams = prompt.getOwnershipParams();
    if (promptParams.getAllowedOwners() == null
        && promptParams.getAllowedOwnershipTypes() == null) {
      // assert that the asset has any owner on it if no specified owners or ownership types
      ObjectNode propertyNode = OBJECT_MAPPER.createObjectNode();
      propertyNode.put("property", "ownership.owners.owner");
      propertyNode.put("operator", "exists");
      return ImmutableList.of(propertyNode);
    }

    List<JsonNode> propertiesList = new ArrayList<>();

    if (promptParams.getAllowedOwners() != null) {
      ObjectNode ownersPropertyNode = OBJECT_MAPPER.createObjectNode();
      ownersPropertyNode.put("property", "ownership.owners.owner");
      ownersPropertyNode.put("operator", "contains_any");
      ArrayNode ownersValues = ownersPropertyNode.putArray("values");
      promptParams.getAllowedOwners().forEach(ownerUrn -> ownersValues.add(ownerUrn.toString()));
      propertiesList.add(ownersPropertyNode);
    }

    if (promptParams.getAllowedOwnershipTypes() != null) {
      ObjectNode ownerTypesPropertyNode = OBJECT_MAPPER.createObjectNode();
      ownerTypesPropertyNode.put("property", "ownership.owners.typeUrn");
      ownerTypesPropertyNode.put("operator", "contains_any");
      ArrayNode ownerTypesValues = ownerTypesPropertyNode.putArray("values");
      promptParams
          .getAllowedOwnershipTypes()
          .forEach(nodeUrn -> ownerTypesValues.add(nodeUrn.toString()));
      propertiesList.add(ownerTypesPropertyNode);
    }

    return propertiesList;
  }

  /*
   * The description field has different aspect paths depending on the entity type.
   * Get the different paths and ensure that one of them exists for this test to pass.
   */
  private static List<JsonNode> buildDocumentationTestConditions(
      @Nonnull final OperationContext opContext) {
    // (1) build a Filter for description
    Filter documentationFilter = new Filter();
    ConjunctiveCriterionArray conjunctiveCriteria = new ConjunctiveCriterionArray();
    conjunctiveCriteria.add(buildFieldExistsConjunctiveCriterion("description"));
    conjunctiveCriteria.add(buildFieldExistsConjunctiveCriterion("editedDescription"));
    conjunctiveCriteria.add(buildFieldExistsConjunctiveCriterion("definition"));
    documentationFilter.setOr(conjunctiveCriteria);

    // (2) get searchable fields to aspect paths map
    final @Nullable Map<String, List<PathSpec>> searchableFieldsToPathSpecs =
        EntitySpecUtils.getSearchableFieldsToPathSpecs(
            opContext.getEntityRegistry(), new ArrayList<>(FORM_ASSIGNMENT_ENTITY_TYPES));

    // (3) convert the filter to test conditions
    return MetadataTestServiceUtils.convertFilterToTestConditions(
        documentationFilter, searchableFieldsToPathSpecs);
  }

  private static ConjunctiveCriterion buildFieldExistsConjunctiveCriterion(
      @Nonnull String fieldName) {
    CriterionArray descriptionCriterionArray = new CriterionArray();
    Criterion descriptionCriterion = CriterionUtils.buildExistsCriterion(fieldName);
    descriptionCriterionArray.add(descriptionCriterion);
    return new ConjunctiveCriterion().setAnd(descriptionCriterionArray);
  }

  private static List<JsonNode> buildGlossaryTermsTestConditions(@Nonnull final FormPrompt prompt) {
    GlossaryTermsParams promptParams = prompt.getGlossaryTermsParams();
    if (promptParams.getAllowedTerms() == null && promptParams.getAllowedTermGroups() == null) {
      // assert that the asset has any glossary term on it if no specified terms or term groups
      ObjectNode propertyNode = OBJECT_MAPPER.createObjectNode();
      propertyNode.put("property", "glossaryTerms.terms.urn");
      propertyNode.put("operator", "exists");
      return ImmutableList.of(propertyNode);
    }

    List<JsonNode> propertiesList = new ArrayList<>();

    if (promptParams.getAllowedTerms() != null) {
      ObjectNode termsPropertyNode = OBJECT_MAPPER.createObjectNode();
      termsPropertyNode.put("property", "glossaryTerms.terms.urn");
      termsPropertyNode.put("operator", "contains_any");
      ArrayNode termsValues = termsPropertyNode.putArray("values");
      promptParams.getAllowedTerms().forEach(termUrn -> termsValues.add(termUrn.toString()));
      propertiesList.add(termsPropertyNode);
    }

    if (promptParams.getAllowedTermGroups() != null) {
      ObjectNode nodesPropertyNode = OBJECT_MAPPER.createObjectNode();
      nodesPropertyNode.put("property", "glossaryTerms.terms.urn.glossaryTermInfo.parentNode");
      nodesPropertyNode.put("operator", "contains_any");
      ArrayNode nodesValues = nodesPropertyNode.putArray("values");
      promptParams.getAllowedTermGroups().forEach(nodeUrn -> nodesValues.add(nodeUrn.toString()));
      propertiesList.add(nodesPropertyNode);
    }

    return propertiesList;
  }

  private static List<JsonNode> buildDomainTestConditions(@Nonnull final FormPrompt prompt) {
    DomainParams promptParams = prompt.getDomainParams();
    if (promptParams == null || promptParams.getAllowedDomains() == null) {
      // assert that the asset has any domain on it if no specified allowed domains
      ObjectNode propertyNode = OBJECT_MAPPER.createObjectNode();
      propertyNode.put("property", "domains.domains");
      propertyNode.put("operator", "exists");
      return ImmutableList.of(propertyNode);
    }

    ObjectNode domainsPropertyNode = OBJECT_MAPPER.createObjectNode();
    domainsPropertyNode.put("property", "domains.domains");
    domainsPropertyNode.put("operator", "contains_any");
    ArrayNode domainValues = domainsPropertyNode.putArray("values");
    promptParams.getAllowedDomains().forEach(domainUrn -> domainValues.add(domainUrn.toString()));
    return ImmutableList.of(domainsPropertyNode);
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
