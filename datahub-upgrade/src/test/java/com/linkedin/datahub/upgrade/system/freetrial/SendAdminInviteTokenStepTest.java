package com.linkedin.datahub.upgrade.system.freetrial;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.invite.InviteTokenService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ControlPlaneService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for SendAdminInviteTokenStep upgrade step. */
public class SendAdminInviteTokenStepTest {

  private static final String ADMIN_ROLE_URN = "urn:li:dataHubRole:Admin";
  private static final String TEST_TOKEN = "test-admin-invite-token-123";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn("SendAdminInviteToken");

  @Mock private OperationContext mockOpContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private InviteTokenService mockInviteTokenService;
  @Mock private ControlPlaneService mockControlPlaneService;
  @Mock private UpgradeContext mockUpgradeContext;

  private SendAdminInviteTokenStep step;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testSuccessfulExecution() throws Exception {
    // Setup
    when(mockInviteTokenService.getEncryptedInviteToken(
            any(OperationContext.class), eq(ADMIN_ROLE_URN), eq(false)))
        .thenReturn(TEST_TOKEN);
    when(mockControlPlaneService.sendAdminInviteToken(TEST_TOKEN, 7, 2)).thenReturn(true);

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    // Execute
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Verify
    assertEquals(result.stepId(), "SendAdminInviteToken");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockInviteTokenService, times(1))
        .getEncryptedInviteToken(any(OperationContext.class), eq(ADMIN_ROLE_URN), eq(false));
    verify(mockControlPlaneService, times(1)).sendAdminInviteToken(TEST_TOKEN, 7, 2);
  }

  @Test
  public void testInviteTokenServiceFailure() throws Exception {
    // Setup - InviteTokenService returns null
    when(mockInviteTokenService.getInviteToken(
            any(OperationContext.class), eq(ADMIN_ROLE_URN), eq(false)))
        .thenReturn(null);

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    // Execute
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Verify
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockControlPlaneService, never()).sendAdminInviteToken(anyString(), anyInt(), anyInt());
  }

  @Test
  public void testControlPlaneServiceFailure() throws Exception {
    // Setup
    when(mockInviteTokenService.getInviteToken(
            any(OperationContext.class), eq(ADMIN_ROLE_URN), eq(false)))
        .thenReturn(TEST_TOKEN);
    when(mockControlPlaneService.sendAdminInviteToken(TEST_TOKEN, 7, 2)).thenReturn(false);

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    // Execute
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Verify
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSkipWhenDisabled() {
    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            false,
            7,
            2,
            false);

    boolean skip = step.skip(mockUpgradeContext);

    assertTrue(skip);
  }

  @Test
  public void testDoNotSkipWhenReprocessEnabled() {
    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            true);

    boolean skip = step.skip(mockUpgradeContext);

    assertFalse(skip);
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(UPGRADE_ID_URN),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(true);

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    boolean skip = step.skip(mockUpgradeContext);

    assertTrue(skip);
  }

  @Test
  public void testDoNotSkipWhenNotPreviouslyRun() {
    when(mockEntityService.exists(
            eq(mockOpContext),
            eq(UPGRADE_ID_URN),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            eq(true)))
        .thenReturn(false);

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    boolean skip = step.skip(mockUpgradeContext);

    assertFalse(skip);
  }

  @Test
  public void testInviteTokenServiceThrowsException() throws Exception {
    // Setup - InviteTokenService throws exception
    when(mockInviteTokenService.getInviteToken(
            any(OperationContext.class), eq(ADMIN_ROLE_URN), eq(false)))
        .thenThrow(new RuntimeException("Service unavailable"));

    step =
        new SendAdminInviteTokenStep(
            mockOpContext,
            mockEntityService,
            mockInviteTokenService,
            mockControlPlaneService,
            true,
            7,
            2,
            false);

    // Execute
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Verify
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
