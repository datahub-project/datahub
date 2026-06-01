package com.linkedin.datahub.upgrade.system.telemetry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.gms.factory.telemetry.TelemetryUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoveClientIdAspectStepTest {

  private static final Urn UPGRADE_URN = BootstrapStep.getUpgradeUrn("remove-unknown-aspects");

  @Mock private EntityService<?> mockEntityService;
  @Mock private OperationContext mockOpContext;
  @Mock private UpgradeContext mockUpgradeContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testSkipsWhenDisabled() {
    RemoveClientIdAspectStep step = new RemoveClientIdAspectStep(mockEntityService, false);

    assertTrue(step.skip(mockUpgradeContext));
    verify(mockEntityService, never()).exists(any(), any(Urn.class), anyBoolean());
  }

  @Test
  public void testSkipsWhenAlreadyRun() {
    when(mockEntityService.exists(eq(mockOpContext), eq(UPGRADE_URN), eq(true))).thenReturn(true);

    RemoveClientIdAspectStep step = new RemoveClientIdAspectStep(mockEntityService, true);

    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testDoesNotSkipWhenNotYetRun() {
    when(mockEntityService.exists(eq(mockOpContext), eq(UPGRADE_URN), eq(true))).thenReturn(false);

    RemoveClientIdAspectStep step = new RemoveClientIdAspectStep(mockEntityService, true);

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSuccessfulExecution() {
    RemoveClientIdAspectStep step = new RemoveClientIdAspectStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockEntityService, times(1))
        .deleteAspect(
            eq(mockOpContext),
            eq(TelemetryUtils.CLIENT_ID_URN),
            eq("clientId"),
            anyMap(),
            eq(true));
    // setUpgradeResult ingests an upgrade aspect so skip() returns true on the next run
    verify(mockEntityService, atLeastOnce())
        .ingestProposal(eq(mockOpContext), any(), any(), eq(false));
  }

  @Test
  public void testFailureHandling() {
    when(mockEntityService.deleteAspect(any(), anyString(), anyString(), anyMap(), anyBoolean()))
        .thenThrow(new RuntimeException("Test exception"));

    RemoveClientIdAspectStep step = new RemoveClientIdAspectStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService, times(1)).deleteUrn(eq(mockOpContext), eq(UPGRADE_URN));
  }
}
