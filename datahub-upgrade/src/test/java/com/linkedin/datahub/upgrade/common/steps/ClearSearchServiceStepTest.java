package com.linkedin.datahub.upgrade.common.steps;

import static com.linkedin.datahub.upgrade.common.Constants.CLEAN_ARG_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ClearSearchServiceStepTest {

  @Mock private EntitySearchService entitySearchService;

  @Mock private UpgradeContext upgradeContext;

  @Mock private UpgradeReport upgradeReport;

  @Mock private OperationContext operationContext;

  private ClearSearchServiceStep clearSearchServiceStep;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(upgradeContext.report()).thenReturn(upgradeReport);
    when(upgradeContext.opContext()).thenReturn(operationContext);
  }

  @Test
  public void testId() {
    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, false);
    assertEquals(clearSearchServiceStep.id(), "ClearSearchServiceStep");
  }

  @Test
  public void testRetryCount() {
    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, false);
    assertEquals(clearSearchServiceStep.retryCount(), 1);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsTrue() {
    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, true);
    assertFalse(clearSearchServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenCleanArgIsPresent() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put(CLEAN_ARG_NAME, Optional.of("true"));

    when(upgradeContext.parsedArgs()).thenReturn(parsedArgs);

    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, false);
    assertFalse(clearSearchServiceStep.skip(upgradeContext));
    verifyNoInteractions(upgradeReport);
  }

  @Test
  public void testSkip_WhenAlwaysRunIsFalseAndNoCleanArg() {
    when(upgradeContext.parsedArgs()).thenReturn(new HashMap<>());

    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, false);
    assertTrue(clearSearchServiceStep.skip(upgradeContext));
    verify(upgradeReport).addLine("Cleanup has not been requested.");
  }

  @Test
  public void testExecutable_Success() {
    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, true);
    Function<UpgradeContext, UpgradeStepResult> executable = clearSearchServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(entitySearchService).clear(operationContext);
    assertEquals(result.stepId(), "ClearSearchServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }

  @Test
  public void testExecutable_Failure() {
    doThrow(new RuntimeException("Test exception"))
        .when(entitySearchService)
        .clear(any(OperationContext.class));

    clearSearchServiceStep = new ClearSearchServiceStep(entitySearchService, true);
    Function<UpgradeContext, UpgradeStepResult> executable = clearSearchServiceStep.executable();

    UpgradeStepResult result = executable.apply(upgradeContext);

    verify(entitySearchService).clear(operationContext);
    verify(upgradeReport).addLine(eq("Failed to clear search service"), any(Exception.class));
    assertEquals(result.stepId(), "ClearSearchServiceStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(result.action(), UpgradeStepResult.Action.CONTINUE);
  }
}
