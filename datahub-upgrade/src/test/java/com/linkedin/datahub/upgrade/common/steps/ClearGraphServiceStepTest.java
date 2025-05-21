package com.linkedin.datahub.upgrade.common.steps;

import static com.linkedin.datahub.upgrade.common.Constants.CLEAN_ARG_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClearGraphServiceStepTest {

  @Mock private GraphService graphService;

  @Mock private UpgradeContext upgradeContext;

  @Mock private UpgradeReport upgradeReport;

  private ClearGraphServiceStep clearGraphServiceStep;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(upgradeContext.report()).thenReturn(upgradeReport);
  }

  @Test
  public void testId() {
    clearGraphServiceStep = new ClearGraphServiceStep(graphService, false);
    assertEquals(clearGraphServiceStep.id(), "ClearGraphServiceStep");
  }

  @Test
  public void testRetryCount() {
    clearGraphServiceStep = new ClearGraphServiceStep(graphService, false);
    assertEquals(clearGraphServiceStep.retryCount(), 1);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsTrue() {
    clearGraphServiceStep = new ClearGraphServiceStep(graphService, true);
    assertFalse(clearGraphServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenCleanArgIsPresent() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(CLEAN_ARG_NAME, Optional.of("true"));

    when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    clearGraphServiceStep = new ClearGraphServiceStep(graphService, false);
    assertFalse(clearGraphServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsFalseAndNoCleanArg() {
    when(upgradeContext.parsedArgs()).thenReturn(new HashMap<>());

    clearGraphServiceStep = new ClearGraphServiceStep(graphService, false);
    assertTrue(clearGraphServiceStep.skip(upgradeContext));
    verify(upgradeReport).addLine("Cleanup has not been requested.");
  }

  @Test
  public void testExecutable_Success() {
    clearGraphServiceStep = new ClearGraphServiceStep(graphService, true);
    Function<UpgradeContext, UpgradeStepResult> executable = clearGraphServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(graphService).clear();
    assertEquals(result.stepId(), "ClearGraphServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }

  @Test
  public void testExecutable_Failure() {
    doThrow(new RuntimeException("Test exception")).when(graphService).clear();

    clearGraphServiceStep = new ClearGraphServiceStep(graphService, true);
    Function<UpgradeContext, UpgradeStepResult> executable = clearGraphServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(graphService).clear();
    verify(upgradeReport).addLine(eq("Failed to clear graph indices"), any(Exception.class));
    assertEquals(result.stepId(), "ClearGraphServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }
}
