package com.linkedin.datahub.upgrade.common.steps;

import static com.linkedin.datahub.upgrade.common.Constants.CLEAN_ARG_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClearSystemMetadataServiceStepTest {

  @Mock private SystemMetadataService systemMetadataService;

  @Mock private UpgradeContext upgradeContext;

  @Mock private UpgradeReport upgradeReport;

  private ClearSystemMetadataServiceStep clearSystemMetadataServiceStep;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(upgradeContext.report()).thenReturn(upgradeReport);
  }

  @Test
  public void testId() {
    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, false);
    assertEquals(clearSystemMetadataServiceStep.id(), "ClearSystemMetadataServiceStep");
  }

  @Test
  public void testRetryCount() {
    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, false);
    assertEquals(clearSystemMetadataServiceStep.retryCount(), 1);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsTrue() {
    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, true);
    assertFalse(clearSystemMetadataServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenCleanArgIsPresent() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(CLEAN_ARG_NAME, Optional.of("true"));

    when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, false);
    assertFalse(clearSystemMetadataServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsFalseAndNoCleanArg() {
    when(upgradeContext.parsedArgs()).thenReturn(new HashMap<>());

    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, false);
    assertTrue(clearSystemMetadataServiceStep.skip(upgradeContext));
    verify(upgradeReport).addLine("Cleanup has not been requested.");
  }

  @Test
  public void testExecutable_Success() {
    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, true);
    Function<UpgradeContext, UpgradeStepResult> executable =
        clearSystemMetadataServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(systemMetadataService).clear();
    assertEquals(result.stepId(), "ClearSystemMetadataServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }

  @Test
  public void testExecutable_Failure() {
    doThrow(new RuntimeException("Test exception")).when(systemMetadataService).clear();

    clearSystemMetadataServiceStep =
        new ClearSystemMetadataServiceStep(systemMetadataService, true);
    Function<UpgradeContext, UpgradeStepResult> executable =
        clearSystemMetadataServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(systemMetadataService).clear();
    verify(upgradeReport)
        .addLine(eq("Failed to clear system metadata service"), any(Exception.class));
    assertEquals(result.stepId(), "ClearSystemMetadataServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }
}
