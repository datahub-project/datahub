package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static org.testng.Assert.*;

import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowEntrypoint;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointArray;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointType;
import com.linkedin.actionworkflow.ActionWorkflowField;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowForm;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowStep;
import com.linkedin.actionworkflow.ActionWorkflowStepActors;
import com.linkedin.actionworkflow.ActionWorkflowStepArray;
import com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment;
import com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignmentType;
import com.linkedin.actionworkflow.ActionWorkflowStepType;
import com.linkedin.actionworkflow.ActionWorkflowTrigger;
import com.linkedin.actionworkflow.ActionWorkflowTriggerType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.PropertyValue;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

public class ActionWorkflowMapperTest {

  private static final String TEST_WORKFLOW_URN = "urn:li:actionRequestWorkflow:test123";
  private static final String TEST_USER_URN = "urn:li:corpuser:testuser";
  private static final String TEST_GROUP_URN = "urn:li:corpGroup:testgroup";
  private static final String TEST_ROLE_URN = "urn:li:dataHubRole:testrole";
  private static final String TEST_OWNERSHIP_TYPE_URN = "urn:li:ownershipType:testownership";

  @Test
  public void testMapBasicWorkflow() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_WORKFLOW_URN);
    assertEquals(result.getName(), "Test Workflow");
    assertEquals(
        result.getCategory(), com.linkedin.datahub.graphql.generated.ActionWorkflowCategory.ACCESS);
    assertEquals(result.getDescription(), "Test workflow description");
    assertNull(result.getCustomCategory());
    assertNotNull(result.getTrigger());
    assertNotNull(result.getTrigger().getForm());
    assertNotNull(result.getTrigger().getForm().getEntrypoints());
    assertNotNull(result.getTrigger().getForm().getEntityTypes());
    assertNotNull(result.getTrigger().getForm().getFields());
    assertNotNull(result.getSteps());
    assertNotNull(result.getCreated());
    assertNotNull(result.getLastModified());
  }

  @Test
  public void testMapWorkflowWithCustomType() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();
    workflowInfo.setCategory(ActionWorkflowCategory.CUSTOM);
    workflowInfo.setCustomCategory("CUSTOM_CATEGORY");

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertEquals(
        result.getCategory(), com.linkedin.datahub.graphql.generated.ActionWorkflowCategory.CUSTOM);
    assertEquals(result.getCustomCategory(), "CUSTOM_CATEGORY");
  }

  @Test
  public void testMapWorkflowWithEntityTypes() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();
    Urn datasetUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    Urn dashboardUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dashboard");

    // Update the entity types in the trigger form
    workflowInfo
        .getTrigger()
        .getForm()
        .setEntityTypes(new UrnArray(Arrays.asList(datasetUrn, dashboardUrn)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getTrigger().getForm().getEntityTypes());
    assertEquals(result.getTrigger().getForm().getEntityTypes().size(), 2);
    assertTrue(
        result.getTrigger().getForm().getEntityTypes().stream()
            .anyMatch(e -> e.toString().equals(EntityType.DATASET.toString())));
    assertTrue(
        result.getTrigger().getForm().getEntityTypes().stream()
            .anyMatch(e -> e.toString().equals(EntityType.DASHBOARD.toString())));
  }

  @Test
  public void testMapWorkflowFields() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    // Add a field with allowed values
    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("testField");
    field.setName("Test Field");
    field.setDescription("Test field description");
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:string"));
    field.setCardinality(PropertyCardinality.SINGLE);
    field.setRequired(true);
    field.setAllowedEntityTypes(new UrnArray(Collections.emptyList()));

    // Add allowed values
    PropertyValue stringValue = new PropertyValue();
    stringValue.setValue(com.linkedin.structured.PrimitivePropertyValue.create("test value"));
    field.setAllowedValues(
        new com.linkedin.structured.PropertyValueArray(Arrays.asList(stringValue)));

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getTrigger().getForm().getFields());
    assertEquals(result.getTrigger().getForm().getFields().size(), 1);

    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);
    assertEquals(resultField.getId(), "testField");
    assertEquals(resultField.getName(), "Test Field");
    assertEquals(resultField.getDescription(), "Test field description");
    assertEquals(
        resultField.getValueType().toString(), ActionWorkflowFieldValueType.STRING.toString());
    assertEquals(resultField.getCardinality().toString(), PropertyCardinality.SINGLE.toString());
    assertTrue(resultField.getRequired());
    assertNotNull(resultField.getAllowedValues());
    assertEquals(resultField.getAllowedValues().size(), 1);
    assertTrue(resultField.getAllowedValues().get(0) instanceof StringValue);
    assertEquals(
        ((StringValue) resultField.getAllowedValues().get(0)).getStringValue(), "test value");
  }

  @Test
  public void testMapWorkflowFieldWithNumberValue() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("numberField");
    field.setName("Number Field");
    // Use a valid URN that contains 'datahub.number' to match the mapper logic
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.number"));
    field.setCardinality(PropertyCardinality.SINGLE);
    field.setRequired(false);
    field.setAllowedEntityTypes(new UrnArray(Collections.emptyList()));

    // Add number value
    PropertyValue numberValue = new PropertyValue();
    numberValue.setValue(com.linkedin.structured.PrimitivePropertyValue.create(42.5));
    field.setAllowedValues(
        new com.linkedin.structured.PropertyValueArray(Arrays.asList(numberValue)));

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);
    assertEquals(
        resultField.getValueType().toString(), ActionWorkflowFieldValueType.NUMBER.toString());
    assertTrue(resultField.getAllowedValues().get(0) instanceof NumberValue);
    assertEquals(((NumberValue) resultField.getAllowedValues().get(0)).getNumberValue(), 42.5f);
  }

  @Test
  public void testMapWorkflowFieldWithUrnValueType() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("urnField");
    field.setName("URN Field");
    // Use a valid URN that contains 'datahub.urn' to match the mapper logic
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.urn"));
    field.setCardinality(PropertyCardinality.MULTIPLE);
    field.setRequired(false);

    // Add allowed entity types
    Urn datasetUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    field.setAllowedEntityTypes(new UrnArray(Arrays.asList(datasetUrn)));

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);
    assertEquals(
        resultField.getValueType().toString(), ActionWorkflowFieldValueType.URN.toString());
    assertEquals(resultField.getCardinality().toString(), PropertyCardinality.MULTIPLE.toString());
    assertNotNull(resultField.getAllowedEntityTypes());
    assertEquals(resultField.getAllowedEntityTypes().size(), 1);
    assertEquals(
        resultField.getAllowedEntityTypes().get(0).toString(), EntityType.DATASET.toString());
  }

  @Test
  public void testMapWorkflowEntrypoints() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowEntrypoint entrypoint1 = new ActionWorkflowEntrypoint();
    entrypoint1.setType(ActionWorkflowEntrypointType.HOME);
    entrypoint1.setLabel("Home Entrypoint");

    ActionWorkflowEntrypoint entrypoint2 = new ActionWorkflowEntrypoint();
    entrypoint2.setType(ActionWorkflowEntrypointType.ENTITY_PROFILE);
    entrypoint2.setLabel("Entity Profile Entrypoint");

    workflowInfo
        .getTrigger()
        .getForm()
        .setEntrypoints(new ActionWorkflowEntrypointArray(Arrays.asList(entrypoint1, entrypoint2)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getTrigger().getForm().getEntrypoints());
    assertEquals(result.getTrigger().getForm().getEntrypoints().size(), 2);

    com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint resultEntrypoint1 =
        result.getTrigger().getForm().getEntrypoints().get(0);
    assertEquals(
        resultEntrypoint1.getType().toString(), ActionWorkflowEntrypointType.HOME.toString());
    assertEquals(resultEntrypoint1.getLabel(), "Home Entrypoint");

    com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint resultEntrypoint2 =
        result.getTrigger().getForm().getEntrypoints().get(1);
    assertEquals(
        resultEntrypoint2.getType().toString(),
        ActionWorkflowEntrypointType.ENTITY_PROFILE.toString());
    assertEquals(resultEntrypoint2.getLabel(), "Entity Profile Entrypoint");
  }

  @Test
  public void testMapWorkflowSteps() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setDescription("Test step description");
    step.setActors(createTestActors());

    workflowInfo.setSteps(new ActionWorkflowStepArray(Arrays.asList(step)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getSteps());
    assertEquals(result.getSteps().size(), 1);

    com.linkedin.datahub.graphql.generated.ActionWorkflowStep resultStep = result.getSteps().get(0);
    assertEquals(resultStep.getId(), "step1");
    assertEquals(resultStep.getType().toString(), ActionWorkflowStepType.APPROVAL.toString());
    assertEquals(resultStep.getDescription(), "Test step description");
    assertNotNull(resultStep.getActors());
  }

  @Test
  public void testMapWorkflowStepActors() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);
    step.setActors(createTestActors());

    workflowInfo.setSteps(new ActionWorkflowStepArray(Arrays.asList(step)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowStepActors actors =
        result.getSteps().get(0).getActors();

    // Check users
    assertNotNull(actors.getUsers());
    assertEquals(actors.getUsers().size(), 1);
    assertEquals(actors.getUsers().get(0).getUrn(), TEST_USER_URN);

    // Check groups
    assertNotNull(actors.getGroups());
    assertEquals(actors.getGroups().size(), 1);
    assertEquals(actors.getGroups().get(0).getUrn(), TEST_GROUP_URN);

    // Check roles
    assertNotNull(actors.getRoles());
    assertEquals(actors.getRoles().size(), 1);
    assertEquals(actors.getRoles().get(0).getUrn(), TEST_ROLE_URN);

    // Check dynamic assignment
    assertNotNull(actors.getDynamicAssignment());
    assertEquals(
        actors.getDynamicAssignment().getType().toString(),
        ActionWorkflowStepDynamicAssignmentType.ENTITY_OWNERS.toString());
    assertNotNull(actors.getDynamicAssignment().getOwnershipTypes());
    assertEquals(actors.getDynamicAssignment().getOwnershipTypes().size(), 1);
    assertEquals(
        actors.getDynamicAssignment().getOwnershipTypes().get(0).getUrn(), TEST_OWNERSHIP_TYPE_URN);
  }

  @Test
  public void testMapAuditStamp() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    long timestamp = System.currentTimeMillis();
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(UrnUtils.getUrn(TEST_USER_URN));
    auditStamp.setTime(timestamp);

    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getCreated());
    assertEquals(result.getCreated().getActor().getUrn(), TEST_USER_URN);
    assertEquals(result.getCreated().getTime(), timestamp);

    assertNotNull(result.getLastModified());
    assertEquals(result.getLastModified().getActor().getUrn(), TEST_USER_URN);
    assertEquals(result.getLastModified().getTime(), timestamp);
  }

  @Test
  public void testMapWorkflowWithEmptyCollections() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();
    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Collections.emptyList()));
    workflowInfo
        .getTrigger()
        .getForm()
        .setEntrypoints(new ActionWorkflowEntrypointArray(Collections.emptyList()));
    workflowInfo.setSteps(new ActionWorkflowStepArray(Collections.emptyList()));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getTrigger().getForm().getFields());
    assertTrue(result.getTrigger().getForm().getFields().isEmpty());
    assertNotNull(result.getTrigger().getForm().getEntrypoints());
    assertTrue(result.getTrigger().getForm().getEntrypoints().isEmpty());
    assertNotNull(result.getSteps());
    assertTrue(result.getSteps().isEmpty());
  }

  @Test
  public void testMapWorkflowStepActorsWithEmptyCollections() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowStep step = new ActionWorkflowStep();
    step.setId("step1");
    step.setType(ActionWorkflowStepType.APPROVAL);

    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();
    actors.setUsers(new UrnArray(Collections.emptyList()));
    actors.setGroups(new UrnArray(Collections.emptyList()));
    actors.setRoles(new UrnArray(Collections.emptyList()));
    actors.setDynamicAssignment(createTestDynamicAssignment());
    step.setActors(actors);

    workflowInfo.setSteps(new ActionWorkflowStepArray(Arrays.asList(step)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowStepActors resultActors =
        result.getSteps().get(0).getActors();
    assertNotNull(resultActors.getUsers());
    assertTrue(resultActors.getUsers().isEmpty());
    assertNotNull(resultActors.getGroups());
    assertTrue(resultActors.getGroups().isEmpty());
    assertNotNull(resultActors.getRoles());
    assertTrue(resultActors.getRoles().isEmpty());
    assertNotNull(resultActors.getDynamicAssignment());
  }

  @Test
  public void testMapWorkflowFieldWithCondition() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("conditionalField");
    field.setName("Conditional Field");
    field.setDescription("Field with condition");
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    field.setCardinality(PropertyCardinality.SINGLE);
    field.setRequired(false);

    // Create condition
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("otherField");
    singleCondition.setValues(new StringArray(Arrays.asList("value1", "value2")));
    singleCondition.setCondition(com.linkedin.metadata.query.filter.Condition.EQUAL);
    singleCondition.setNegated(false);

    condition.setSingleFieldValueCondition(singleCondition);
    field.setCondition(condition);

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    assertNotNull(result.getTrigger().getForm().getFields());
    assertEquals(result.getTrigger().getForm().getFields().size(), 1);

    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);
    assertEquals(resultField.getId(), "conditionalField");
    assertEquals(resultField.getName(), "Conditional Field");
    assertEquals(resultField.getDescription(), "Field with condition");
    assertEquals(
        resultField.getValueType().toString(), ActionWorkflowFieldValueType.STRING.toString());
    assertEquals(resultField.getCardinality().toString(), PropertyCardinality.SINGLE.toString());
    assertFalse(resultField.getRequired());

    // Verify condition
    assertNotNull(resultField.getCondition());
    assertEquals(
        resultField.getCondition().getType().toString(),
        ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE.toString());

    com.linkedin.datahub.graphql.generated.ActionWorkflowSingleFieldValueCondition
        resultSingleCondition = resultField.getCondition().getSingleFieldValueCondition();
    assertNotNull(resultSingleCondition);
    assertEquals(resultSingleCondition.getField(), "otherField");
    assertEquals(resultSingleCondition.getValues(), Arrays.asList("value1", "value2"));
    assertEquals(resultSingleCondition.getCondition().toString(), FilterOperator.EQUAL.toString());
    assertFalse(resultSingleCondition.getNegated());
  }

  @Test
  public void testMapWorkflowFieldWithNegatedCondition() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("negatedField");
    field.setName("Negated Field");
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    field.setCardinality(PropertyCardinality.SINGLE);
    field.setRequired(false);

    // Create negated condition
    com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();
    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE);

    com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();
    singleCondition.setField("approvalType");
    singleCondition.setValues(new StringArray(Arrays.asList("AUTOMATIC")));
    singleCondition.setCondition(com.linkedin.metadata.query.filter.Condition.EQUAL);
    singleCondition.setNegated(true);

    condition.setSingleFieldValueCondition(singleCondition);
    field.setCondition(condition);

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);

    // Verify negated condition
    assertNotNull(resultField.getCondition());
    com.linkedin.datahub.graphql.generated.ActionWorkflowSingleFieldValueCondition
        resultSingleCondition = resultField.getCondition().getSingleFieldValueCondition();
    assertNotNull(resultSingleCondition);
    assertEquals(resultSingleCondition.getField(), "approvalType");
    assertEquals(resultSingleCondition.getValues(), Arrays.asList("AUTOMATIC"));
    assertEquals(resultSingleCondition.getCondition().toString(), FilterOperator.EQUAL.toString());
    assertTrue(resultSingleCondition.getNegated());
  }

  @Test
  public void testMapWorkflowFieldWithoutCondition() {
    // GIVEN
    Urn workflowUrn = UrnUtils.getUrn(TEST_WORKFLOW_URN);
    ActionWorkflowInfo workflowInfo = createBasicWorkflowInfo();

    ActionWorkflowField field = new ActionWorkflowField();
    field.setId("noConditionField");
    field.setName("No Condition Field");
    field.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    field.setCardinality(PropertyCardinality.SINGLE);
    field.setRequired(false);
    // No condition set

    workflowInfo
        .getTrigger()
        .getForm()
        .setFields(new ActionWorkflowFieldArray(Arrays.asList(field)));

    // WHEN
    ActionWorkflow result = ActionWorkflowMapper.map(workflowUrn, workflowInfo);

    // THEN
    com.linkedin.datahub.graphql.generated.ActionWorkflowField resultField =
        result.getTrigger().getForm().getFields().get(0);

    // Verify no condition
    assertNull(resultField.getCondition());
  }

  // Helper methods
  private ActionWorkflowInfo createBasicWorkflowInfo() {
    ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName("Test Workflow");
    workflowInfo.setCategory(ActionWorkflowCategory.ACCESS);
    workflowInfo.setDescription("Test workflow description");

    // Create trigger with form
    ActionWorkflowTrigger trigger = new ActionWorkflowTrigger();
    trigger.setType(ActionWorkflowTriggerType.FORM_SUBMITTED);

    ActionWorkflowForm form = new ActionWorkflowForm();
    form.setEntityTypes(new UrnArray(Collections.emptyList()));
    form.setFields(new ActionWorkflowFieldArray(Collections.emptyList()));
    form.setEntrypoints(new ActionWorkflowEntrypointArray(Collections.emptyList()));

    trigger.setForm(form);
    workflowInfo.setTrigger(trigger);

    workflowInfo.setSteps(new ActionWorkflowStepArray(Collections.emptyList()));

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(UrnUtils.getUrn(TEST_USER_URN));
    auditStamp.setTime(System.currentTimeMillis());
    workflowInfo.setCreated(auditStamp);
    workflowInfo.setLastModified(auditStamp);

    return workflowInfo;
  }

  private ActionWorkflowStepActors createTestActors() {
    ActionWorkflowStepActors actors = new ActionWorkflowStepActors();

    // Set users
    actors.setUsers(new UrnArray(Arrays.asList(UrnUtils.getUrn(TEST_USER_URN))));

    // Set groups
    actors.setGroups(new UrnArray(Arrays.asList(UrnUtils.getUrn(TEST_GROUP_URN))));

    // Set roles
    actors.setRoles(new UrnArray(Arrays.asList(UrnUtils.getUrn(TEST_ROLE_URN))));

    // Set dynamic assignment
    actors.setDynamicAssignment(createTestDynamicAssignment());

    return actors;
  }

  private ActionWorkflowStepDynamicAssignment createTestDynamicAssignment() {
    ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new ActionWorkflowStepDynamicAssignment();
    dynamicAssignment.setType(ActionWorkflowStepDynamicAssignmentType.ENTITY_OWNERS);
    dynamicAssignment.setOwnershipTypeUrns(
        new UrnArray(Arrays.asList(UrnUtils.getUrn(TEST_OWNERSHIP_TYPE_URN))));
    return dynamicAssignment;
  }
}
