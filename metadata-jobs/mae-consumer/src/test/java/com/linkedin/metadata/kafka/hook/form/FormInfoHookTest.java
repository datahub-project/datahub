package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.AssignmentStatus;
import com.linkedin.form.FormAssignmentStatus;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormSettings;
import com.linkedin.form.FormState;
import com.linkedin.form.FormStatus;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormInfoHookTest {

  private static final String TEST_FORM_URN = "urn:li:form:test-form";
  private static final String TEST_CONSUMER_GROUP = "test-consumer-group";

  private SystemEntityClient mockEntityClient;
  private OperationContext mockOperationContext;
  private FormInfoHook hook;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    mockOperationContext = mock(OperationContext.class);
    hook = new FormInfoHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    hook.init(mockOperationContext);
  }

  @Test
  public void testIsEnabled() {
    FormInfoHook enabledHook = new FormInfoHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    FormInfoHook disabledHook = new FormInfoHook(false, mockEntityClient, TEST_CONSUMER_GROUP);

    assertTrue(enabledHook.isEnabled());
    assertFalse(disabledHook.isEnabled());
  }

  @Test
  public void testIsEligibleForProcessing() {
    // Test with valid form info update
    MetadataChangeLog validEvent =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT);

    assertTrue(hook.isEnabled() && hook.isEligibleForProcessing(validEvent));

    // Test with invalid aspect name
    MetadataChangeLog invalidAspectEvent =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName("invalid-aspect")
            .setChangeType(ChangeType.UPSERT);

    assertFalse(hook.isEligibleForProcessing(invalidAspectEvent));

    // Test with invalid change type
    MetadataChangeLog invalidChangeTypeEvent =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.DELETE);

    assertFalse(hook.isEligibleForProcessing(invalidChangeTypeEvent));
  }

  @Test
  public void testHandleFormPublish() throws Exception {
    // Create form info with published state
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));

    // Create previous form info with draft state
    FormInfo prevFormInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.DRAFT));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response for form assignment status and formSettings
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that the entity client was called to ingest the proposal
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOperationContext), proposalCaptor.capture(), eq(false));

    // Verify the proposal
    MetadataChangeProposal capturedProposal = proposalCaptor.getValue();
    assertEquals(capturedProposal.getEntityType(), EXECUTION_REQUEST_ENTITY_NAME);
    assertEquals(capturedProposal.getAspectName(), EXECUTION_REQUEST_INPUT_ASPECT_NAME);
  }

  @Test
  public void testHandleFormPublishWhileAssigning() throws Exception {
    // Create form info with published state
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));

    // Create previous form info with draft state
    FormInfo prevFormInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.DRAFT));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response for form assignment status with IN_PROGRESS state
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.IN_PROGRESS);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleFormPublishWithActorChanges() throws Exception {
    // Create form info with published state and some actors
    FormInfo formInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.PUBLISHED))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:newUser"))))
                    .setOwners(false));

    // Create previous form info with published state but different actors
    FormInfo prevFormInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.PUBLISHED))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:oldUser"))))
                    .setOwners(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response for form assignment status and formSettings
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that the entity client was called to ingest the proposal
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityClient, times(1))
        .ingestProposal(eq(mockOperationContext), proposalCaptor.capture(), eq(false));

    // Verify the proposal
    MetadataChangeProposal capturedProposal = proposalCaptor.getValue();
    assertEquals(capturedProposal.getEntityType(), EXECUTION_REQUEST_ENTITY_NAME);
    assertEquals(capturedProposal.getAspectName(), EXECUTION_REQUEST_INPUT_ASPECT_NAME);
  }

  @Test
  public void testHandleFormPublishWithoutActorChanges() throws Exception {
    // Create form info with published state and same actors
    FormInfo formInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.PUBLISHED))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:sameUser"))))
                    .setOwners(false));

    // Create previous form info with published state and same actors
    FormInfo prevFormInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.PUBLISHED))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:sameUser"))))
                    .setOwners(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response for form assignment status and formSettings
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since actors haven't changed
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleFormDraftWithActorChanges() throws Exception {
    // Create form info with draft state and some actors
    FormInfo formInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.DRAFT))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:newUser"))))
                    .setOwners(false));

    // Create previous form info with draft state but different actors
    FormInfo prevFormInfo =
        new FormInfo()
            .setStatus(new FormStatus().setState(FormState.DRAFT))
            .setActors(
                new com.linkedin.form.FormActorAssignment()
                    .setUsers(
                        new com.linkedin.common.UrnArray(
                            Collections.singletonList(UrnUtils.getUrn("urn:li:corpuser:oldUser"))))
                    .setOwners(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response for form assignment status and formSettings
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since form is not published
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleFormPublishWithNotificationsDisabled() throws Exception {
    // Create form info with published state
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));

    // Create previous form info with draft state
    FormInfo prevFormInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.DRAFT));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_INFO_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formInfo))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormInfo));

    // Mock the entity client response with notifications disabled and no active assignments
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings()
                    .setNotifyAssigneesOnPublish(false));

    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));

    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since notifications are disabled
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }
}
