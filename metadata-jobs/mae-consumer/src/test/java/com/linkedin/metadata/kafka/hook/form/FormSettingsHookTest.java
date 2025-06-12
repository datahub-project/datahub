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
import com.linkedin.form.FormNotificationSettings;
import com.linkedin.form.FormSettings;
import com.linkedin.form.FormState;
import com.linkedin.form.FormStatus;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormSettingsHookTest {

  private static final String TEST_FORM_URN = "urn:li:form:test-form";
  private static final String TEST_CONSUMER_GROUP = "test-consumer-group";

  private SystemEntityClient mockEntityClient;
  private OperationContext mockOperationContext;
  private FormSettingsHook hook;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    mockOperationContext = mock(OperationContext.class);
    hook = new FormSettingsHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    hook.init(mockOperationContext);
  }

  @Test
  public void testIsEnabled() {
    FormSettingsHook enabledHook =
        new FormSettingsHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    FormSettingsHook disabledHook =
        new FormSettingsHook(false, mockEntityClient, TEST_CONSUMER_GROUP);

    assertTrue(enabledHook.isEnabled());
    assertFalse(disabledHook.isEnabled());
  }

  @Test
  public void testIsEligibleForProcessing() {
    // Test with valid form settings update
    MetadataChangeLog validEvent =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
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
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.DELETE);

    assertFalse(hook.isEligibleForProcessing(invalidChangeTypeEvent));
  }

  @Test
  public void testHandleEnableNotifications() throws Exception {
    // Create form settings with notifications enabled
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    // Create previous form settings with notifications disabled
    FormSettings prevFormSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formSettings))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormSettings));

    // Mock the entity client response for form info and assignment status
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
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
  public void testHandleEnableNotificationsPreviousNullAspect() throws Exception {
    // Create form settings with notifications enabled
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    // Create the event with no previous aspect value
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formSettings));

    // Mock the entity client response for form info and assignment status
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
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
  public void testHandleEnableNotificationsWhileAssigning() throws Exception {
    // Create form settings with notifications enabled
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    // Create previous form settings with notifications disabled
    FormSettings prevFormSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formSettings))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormSettings));

    // Mock the entity client response for form info and assignment status with IN_PROGRESS state
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.IN_PROGRESS);
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since form is currently assigning
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleEnableNotificationsForDraftForm() throws Exception {
    // Create form settings with notifications enabled
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    // Create previous form settings with notifications disabled
    FormSettings prevFormSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(false));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formSettings))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormSettings));

    // Mock the entity client response for form info and assignment status
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.DRAFT));
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since form is in draft state
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleDisableNotifications() throws Exception {
    // Create form settings with notifications disabled
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(false));

    // Create previous form settings with notifications enabled
    FormSettings prevFormSettings =
        new FormSettings()
            .setNotificationSettings(
                new FormNotificationSettings().setNotifyAssigneesOnPublish(true));

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_SETTINGS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(formSettings))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevFormSettings));

    // Mock the entity client response for form info and assignment status
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_ASSIGNMENT_STATUS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(assignmentStatus.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Wait for async processing
    Thread.sleep(4000);

    // Verify that no proposal was ingested since notifications were disabled
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }
}
