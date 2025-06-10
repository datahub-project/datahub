package com.linkedin.datahub.graphql.types.form;

import static org.testng.Assert.*;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.form.FormActorAssignment;
import com.linkedin.form.FormAssignmentStatus;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormNotificationSettings;
import com.linkedin.form.FormPrompt;
import com.linkedin.form.FormPromptArray;
import com.linkedin.form.FormStatus;
import com.linkedin.metadata.Constants;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class FormMapperTest {

  @Test
  public void testGetSuccess() throws Exception {
    com.linkedin.form.FormSettings formSettings =
        new com.linkedin.form.FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        Constants.FORM_SETTINGS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(formSettings.data())));
    EntityResponse response = new EntityResponse().setAspects(new EnvelopedAspectMap(aspects));
    response.setUrn(Urn.createFromString("urn:li:form:test"));

    Form form = FormMapper.map(null, response);

    assertTrue(form.getFormSettings().getNotificationSettings().getNotifyAssigneesOnPublish());
  }

  @Test
  public void testMapFormInfo() {
    // Create test data
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test-form");
    FormInfo gmsFormInfo = new FormInfo();
    gmsFormInfo.setName("Test Form");
    gmsFormInfo.setType(com.linkedin.form.FormType.COMPLETION);
    gmsFormInfo.setDescription("Test Description");

    // Create form prompts
    FormPrompt prompt = new FormPrompt();
    prompt.setId("prompt1");
    prompt.setTitle("Test Prompt");
    prompt.setType(com.linkedin.form.FormPromptType.STRUCTURED_PROPERTY);
    prompt.setRequired(true);
    gmsFormInfo.setPrompts(new FormPromptArray());
    gmsFormInfo.getPrompts().add(prompt);

    // Create form actors
    FormActorAssignment actors = new FormActorAssignment();
    actors.setOwners(true);
    gmsFormInfo.setActors(actors);

    // Create form status
    FormStatus status = new FormStatus();
    status.setState(com.linkedin.form.FormState.DRAFT);
    gmsFormInfo.setStatus(status);

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(formUrn);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(gmsFormInfo.data()));
    aspectMap.put(Constants.FORM_INFO_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(aspectMap);

    // Map the form
    Form result = FormMapper.map(null, entityResponse);

    // Verify the mapping
    assertNotNull(result);
    assertEquals(result.getUrn(), formUrn.toString());
    assertEquals(result.getType(), EntityType.FORM);
    assertNotNull(result.getInfo());
    assertEquals(result.getInfo().getName(), "Test Form");
    assertEquals(
        result.getInfo().getType(), com.linkedin.datahub.graphql.generated.FormType.COMPLETION);
    assertEquals(result.getInfo().getDescription(), "Test Description");
    assertNotNull(result.getInfo().getPrompts());
    assertEquals(result.getInfo().getPrompts().size(), 1);
    assertEquals(result.getInfo().getPrompts().get(0).getId(), "prompt1");
    assertEquals(result.getInfo().getPrompts().get(0).getTitle(), "Test Prompt");
    assertEquals(
        result.getInfo().getPrompts().get(0).getType(), FormPromptType.STRUCTURED_PROPERTY);
    assertTrue(result.getInfo().getPrompts().get(0).getRequired());
    assertNotNull(result.getInfo().getActors());
    assertTrue(result.getInfo().getActors().getOwners());
    assertNotNull(result.getInfo().getStatus());
    assertEquals(result.getInfo().getStatus().getState(), FormState.DRAFT);
  }

  @Test
  public void testMapDynamicFormAssignment() {
    // Create test data
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test-form");
    DynamicFormAssignment gmsFormAssignment = new DynamicFormAssignment();
    gmsFormAssignment.setJson("{\"filter\": \"test\"}");

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(formUrn);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(gmsFormAssignment.data()));
    aspectMap.put(Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(aspectMap);

    // Map the form
    Form result = FormMapper.map(null, entityResponse);

    // Verify the mapping
    assertNotNull(result);
    assertNotNull(result.getDynamicFormAssignment());
    assertEquals(result.getDynamicFormAssignment().getJson(), "{\"filter\": \"test\"}");
  }

  @Test
  public void testMapFormAssignmentStatus() {
    // Create test data
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test-form");
    FormAssignmentStatus gmsFormAssignmentStatus = new FormAssignmentStatus();
    gmsFormAssignmentStatus.setStatus(com.linkedin.form.AssignmentStatus.IN_PROGRESS);
    gmsFormAssignmentStatus.setTimestamp(1234567890L);

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(formUrn);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(gmsFormAssignmentStatus.data()));
    aspectMap.put(Constants.FORM_ASSIGNMENT_STATUS_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(aspectMap);

    // Map the form
    Form result = FormMapper.map(null, entityResponse);

    // Verify the mapping
    assertNotNull(result);
    assertNotNull(result.getFormAssignmentStatus());
    assertEquals(result.getFormAssignmentStatus().getStatus(), AssignmentStatus.IN_PROGRESS);
    assertEquals(result.getFormAssignmentStatus().getTimestamp(), Long.valueOf(1234567890L));
  }

  @Test
  public void testMapOwnership() {
    // Create test data
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test-form");
    Ownership ownership = new Ownership();
    ownership.setOwners(new OwnerArray());

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(formUrn);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(ownership.data()));
    aspectMap.put(Constants.OWNERSHIP_ASPECT_NAME, envelopedAspect);
    entityResponse.setAspects(aspectMap);

    // Map the form
    Form result = FormMapper.map(null, entityResponse);

    // Verify the mapping
    assertNotNull(result);
    assertNotNull(result.getOwnership());
    assertNotNull(result.getOwnership().getOwners());
    assertTrue(result.getOwnership().getOwners().isEmpty());
  }

  @Test
  public void testDefaultForm() {
    // Create test data with no aspects
    Urn formUrn = UrnUtils.getUrn("urn:li:form:test-form");
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(formUrn);
    entityResponse.setAspects(new EnvelopedAspectMap());

    // Map the form
    Form result = FormMapper.map(null, entityResponse);

    // Verify the default form
    assertNotNull(result);
    assertEquals(result.getUrn(), formUrn.toString());
    assertEquals(result.getType(), EntityType.FORM);
    assertNotNull(result.getInfo());
    assertEquals(result.getInfo().getName(), "");
    assertEquals(
        result.getInfo().getType(), com.linkedin.datahub.graphql.generated.FormType.COMPLETION);
    assertNotNull(result.getInfo().getPrompts());
    assertTrue(result.getInfo().getPrompts().isEmpty());
    assertNotNull(result.getInfo().getActors());
    assertFalse(result.getInfo().getActors().getOwners());
    assertNotNull(result.getInfo().getStatus());
    assertEquals(result.getInfo().getStatus().getState(), FormState.DRAFT);
  }
}
