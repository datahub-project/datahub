package com.linkedin.datahub.upgrade.system.criterion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CriterionFilterAspectsBlockingTest {

  @Test
  public void whenDisabled_stepsEmpty() {
    CriterionFilterAspectsBlocking up =
        new CriterionFilterAspectsBlocking(
            Mockito.mock(OperationContext.class),
            Mockito.mock(EntityService.class),
            Mockito.mock(AspectDao.class),
            "test-version",
            false,
            500,
            1000,
            0);
    assertTrue(up.steps().isEmpty());
  }

  @Test
  public void whenEnabled_singleStep() {
    CriterionFilterAspectsBlocking up =
        new CriterionFilterAspectsBlocking(
            Mockito.mock(OperationContext.class),
            Mockito.mock(EntityService.class),
            Mockito.mock(AspectDao.class),
            "test-version",
            true,
            500,
            1000,
            0);
    List<UpgradeStep> steps = up.steps();
    assertEquals(steps.size(), 1);
    assertEquals(steps.get(0).id(), CriterionFilterAspectsBlockingStep.stepId("test-version"));
  }

  @Test
  public void idIsFullyQualifiedClassName() {
    CriterionFilterAspectsBlocking up =
        new CriterionFilterAspectsBlocking(
            Mockito.mock(OperationContext.class),
            Mockito.mock(EntityService.class),
            Mockito.mock(AspectDao.class),
            "test-version",
            true,
            500,
            1000,
            0);
    assertEquals(up.id(), CriterionFilterAspectsBlocking.class.getName());
  }
}
