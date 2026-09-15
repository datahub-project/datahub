package com.linkedin.datahub.upgrade.kubernetes;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import uk.org.webcompere.systemstubs.testng.SystemStubsListener;

@Listeners(SystemStubsListener.class)
public class ScaleDownEvaluationStepTest {

  @Mock private UpgradeContext upgradeContext;
  @Mock private BlockingSystemUpgrade upgradeVoter;
  @Mock private BlockingSystemUpgrade upgradeNonVoter;

  private ScaleDownEvaluationStep step;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testId() {
    step = new ScaleDownEvaluationStep(List.of());
    assertEquals(step.id(), "ScaleDownEvaluationStep");
  }

  @Test
  public void testExecutableSetsScaleDownRequiredFalseWhenNoUpgrades() {
    step = new ScaleDownEvaluationStep(List.of());
    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeContext).setScaleDownRequired(false);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableSetsScaleDownRequiredFalseWhenNoOneVotes() {
    when(upgradeNonVoter.id()).thenReturn("SomeUpgrade");
    when(upgradeNonVoter.requiresK8ScaleDown(upgradeContext)).thenReturn(false);
    step = new ScaleDownEvaluationStep(List.of(upgradeNonVoter));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeContext).setScaleDownRequired(false);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableSetsScaleDownRequiredTrueWhenOneVotesAndExitsEarly() {
    when(upgradeVoter.id()).thenReturn("BuildIndices");
    when(upgradeVoter.requiresK8ScaleDown(upgradeContext)).thenReturn(true);
    step = new ScaleDownEvaluationStep(List.of(upgradeVoter, upgradeNonVoter));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeContext).setScaleDownRequired(true);
    verify(upgradeNonVoter, never()).requiresK8ScaleDown(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableSkipsKubernetesScaleDownUpgrade() {
    when(upgradeVoter.id()).thenReturn("KubernetesScaleDown");
    when(upgradeVoter.requiresK8ScaleDown(upgradeContext)).thenReturn(true);
    step = new ScaleDownEvaluationStep(List.of(upgradeVoter));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeVoter, never()).requiresK8ScaleDown(upgradeContext);
    verify(upgradeContext).setScaleDownRequired(false);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableCatchesExceptionAndContinues() {
    when(upgradeVoter.id()).thenReturn("First");
    when(upgradeVoter.requiresK8ScaleDown(upgradeContext))
        .thenThrow(new RuntimeException("ES down"));
    when(upgradeNonVoter.id()).thenReturn("Second");
    when(upgradeNonVoter.requiresK8ScaleDown(upgradeContext)).thenReturn(false);
    step = new ScaleDownEvaluationStep(List.of(upgradeVoter, upgradeNonVoter));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeContext).setScaleDownRequired(false);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableReturnsSucceededWhenVoterThrowsThenNextVotesTrue() {
    when(upgradeVoter.id()).thenReturn("First");
    when(upgradeVoter.requiresK8ScaleDown(upgradeContext)).thenThrow(new RuntimeException("oops"));
    when(upgradeNonVoter.id()).thenReturn("Second");
    when(upgradeNonVoter.requiresK8ScaleDown(upgradeContext)).thenReturn(true);
    step = new ScaleDownEvaluationStep(List.of(upgradeVoter, upgradeNonVoter));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    verify(upgradeContext).setScaleDownRequired(true);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }
}
