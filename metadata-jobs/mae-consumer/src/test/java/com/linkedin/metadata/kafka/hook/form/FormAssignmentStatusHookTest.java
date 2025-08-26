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
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormAssignmentStatusHookTest {

  private static final String TEST_FORM_URN = "urn:li:form:test-form";
  private static final String TEST_CONSUMER_GROUP = "test-consumer-group";

  private SystemEntityClient mockEntityClient;
  private OperationContext mockOperationContext;
  private FormAssignmentStatusHook hook;

  @BeforeMethod
  public void setup() {
    mockEntityClient = mock(SystemEntityClient.class);
    mockOperationContext = mock(OperationContext.class);
    hook = new FormAssignmentStatusHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    hook.init(mockOperationContext);
  }

  @Test
  public void testIsEnabled() {
    FormAssignmentStatusHook enabledHook =
        new FormAssignmentStatusHook(true, mockEntityClient, TEST_CONSUMER_GROUP);
    FormAssignmentStatusHook disabledHook =
        new FormAssignmentStatusHook(false, mockEntityClient, TEST_CONSUMER_GROUP);

    assertTrue(enabledHook.isEnabled());
    assertFalse(disabledHook.isEnabled());
  }

  @Test
  public void testIsEligibleForProcessing() {
    // Test with valid form assignment status update
    MetadataChangeLog validEvent =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
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
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
            .setChangeType(ChangeType.DELETE);

    assertFalse(hook.isEligibleForProcessing(invalidChangeTypeEvent));
  }

  @Test
  public void testHandleAssignmentCompleteAndFormPublished() throws Exception {
    // Create form assignment status with COMPLETE state
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(assignmentStatus));

    // Mock the entity client response for form info and form settings
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_INFO_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

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
  public void testHandleAssignmentCompleteButNotificationsDisabled() throws Exception {
    // Create form assignment status with COMPLETE state
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(assignmentStatus));

    // Mock the entity client response for form info and form settings with notifications disabled
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.PUBLISHED));
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
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_INFO_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Verify that no proposal was ingested since notifications are disabled
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleAssignmentCompleteButFormNotPublished() throws Exception {
    // Create form assignment status with COMPLETE state
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.COMPLETE);

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(assignmentStatus));

    // Mock the entity client response for form info with DRAFT state and form settings
    FormInfo formInfo = new FormInfo().setStatus(new FormStatus().setState(FormState.DRAFT));
    FormSettings formSettings =
        new FormSettings()
            .setNotificationSettings(
                new com.linkedin.form.FormNotificationSettings().setNotifyAssigneesOnPublish(true));
    EntityResponse response =
        new EntityResponse()
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        FORM_INFO_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formInfo.data())),
                        FORM_SETTINGS_ASPECT_NAME,
                        new com.linkedin.entity.EnvelopedAspect()
                            .setValue(new Aspect(formSettings.data())))));
    when(mockEntityClient.getV2(
            eq(mockOperationContext),
            eq(UrnUtils.getUrn(TEST_FORM_URN)),
            eq(ImmutableSet.of(FORM_SETTINGS_ASPECT_NAME, FORM_INFO_ASPECT_NAME))))
        .thenReturn(response);

    // Invoke the hook
    hook.invoke(event);

    // Verify that no proposal was ingested since form is not published
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testHandleAssignmentInProgress() throws Exception {
    // Create form assignment status with IN_PROGRESS state
    FormAssignmentStatus assignmentStatus =
        new FormAssignmentStatus().setStatus(AssignmentStatus.IN_PROGRESS);

    // Create the event
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setEntityUrn(UrnUtils.getUrn(TEST_FORM_URN))
            .setAspectName(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)
            .setChangeType(ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(assignmentStatus));

    // Invoke the hook
    hook.invoke(event);

    // Verify that no proposal was ingested and no form info/settings were fetched
    verify(mockEntityClient, never()).getV2(any(), any(), any());
    verify(mockEntityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }
}
