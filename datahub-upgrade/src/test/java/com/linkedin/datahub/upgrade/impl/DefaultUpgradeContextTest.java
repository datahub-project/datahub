package com.linkedin.datahub.upgrade.impl;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeReport;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultUpgradeContextTest {

  @Mock private OperationContext opContext;
  @Mock private UpgradeReport report;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testScaleDownRequiredDefaultsFalse() {
    DefaultUpgradeContext context =
        new DefaultUpgradeContext(opContext, null, report, List.of(), List.of());

    assertFalse(context.isScaleDownRequired());
  }

  @Test
  public void testSetScaleDownRequiredAndIsScaleDownRequired() {
    DefaultUpgradeContext context =
        new DefaultUpgradeContext(opContext, null, report, List.of(), List.of());

    context.setScaleDownRequired(true);
    assertTrue(context.isScaleDownRequired());

    context.setScaleDownRequired(false);
    assertFalse(context.isScaleDownRequired());
  }
}
