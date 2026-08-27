package com.linkedin.datahub.upgrade.system.globalsettings;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestDefaultGlobalSettingsUpgradeStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
  }

  @Test
  public void testSkipWhenDisabled() {
    IngestDefaultGlobalSettingsUpgradeStep step =
        new IngestDefaultGlobalSettingsUpgradeStep(mockEntityService, false);

    assertTrue(step.skip(mockUpgradeContext));
    verify(mockEntityService, never()).ingestProposal(any(), any(), any(), eq(false));
  }

  @Test
  public void testNoSkipWhenEnabled() {
    IngestDefaultGlobalSettingsUpgradeStep step =
        new IngestDefaultGlobalSettingsUpgradeStep(
            mockEntityService, true, "boot/test_global_settings.json");

    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceeds() throws Exception {
    // Use the minimal test resource; stub getAspect to return null so merge yields default only
    when(mockEntityService.getAspect(any(), any(), any(), eq(0))).thenReturn(null);

    IngestDefaultGlobalSettingsUpgradeStep step =
        new IngestDefaultGlobalSettingsUpgradeStep(
            mockEntityService, true, "boot/test_global_settings.json");

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService).ingestProposal(any(OperationContext.class), any(), any(), eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableFailsOnException() {
    // ingestProposal throws — the step must catch and return FAILED
    when(mockEntityService.getAspect(any(), any(), any(), eq(0))).thenReturn(null);
    when(mockEntityService.ingestProposal(any(), any(), any(), eq(false)))
        .thenThrow(new RuntimeException("simulated failure"));

    IngestDefaultGlobalSettingsUpgradeStep step =
        new IngestDefaultGlobalSettingsUpgradeStep(
            mockEntityService, true, "boot/test_global_settings.json");

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
