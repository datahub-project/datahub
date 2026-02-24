package com.linkedin.datahub.upgrade.system.retention;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RetentionService;
import org.testng.annotations.Test;

public class IngestRetentionPoliciesTest {

  @Test
  public void testId() {
    IngestRetentionPolicies upgrade =
        new IngestRetentionPolicies(
            mock(RetentionService.class), mock(EntityService.class), true, false, "");
    assertEquals(upgrade.id(), "IngestRetentionPolicies");
  }

  @Test
  public void testStepsAlwaysSingleStep() {
    IngestRetentionPolicies enabled =
        new IngestRetentionPolicies(
            mock(RetentionService.class), mock(EntityService.class), true, false, "");
    assertEquals(enabled.steps().size(), 1);

    IngestRetentionPolicies disabled =
        new IngestRetentionPolicies(
            mock(RetentionService.class), mock(EntityService.class), false, false, "");
    assertEquals(disabled.steps().size(), 1);
  }

  @Test
  public void testStepSkipsWhenDisabled() {
    IngestRetentionPolicies upgrade =
        new IngestRetentionPolicies(
            mock(RetentionService.class), mock(EntityService.class), false, false, "");
    UpgradeContext mockContext = mock(UpgradeContext.class);
    assertTrue(upgrade.steps().get(0).skip(mockContext));
  }

  @Test
  public void testStepDoesNotSkipWhenEnabled() {
    IngestRetentionPolicies upgrade =
        new IngestRetentionPolicies(
            mock(RetentionService.class), mock(EntityService.class), true, false, "");
    UpgradeContext mockContext = mock(UpgradeContext.class);
    assertFalse(upgrade.steps().get(0).skip(mockContext));
  }
}
