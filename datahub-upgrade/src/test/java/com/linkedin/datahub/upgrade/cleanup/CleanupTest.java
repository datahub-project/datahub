package com.linkedin.datahub.upgrade.cleanup;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CleanupTest {

  @Test
  public void testId() {
    Cleanup cleanup = new Cleanup(Collections.emptyList());
    assertEquals(cleanup.id(), "Cleanup");
  }

  @Test
  public void testStepsReturnsProvidedSteps() {
    UpgradeStep step1 = Mockito.mock(UpgradeStep.class);
    UpgradeStep step2 = Mockito.mock(UpgradeStep.class);
    List<UpgradeStep> steps = Arrays.asList(step1, step2);

    Cleanup cleanup = new Cleanup(steps);

    assertEquals(cleanup.steps().size(), 2);
    assertEquals(cleanup.steps(), steps);
  }

  @Test
  public void testEmptySteps() {
    Cleanup cleanup = new Cleanup(Collections.emptyList());
    assertTrue(cleanup.steps().isEmpty());
  }
}
